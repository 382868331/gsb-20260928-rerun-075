"""基于已到达观察进行不可逆、可审计的自适应干预。"""

from dataclasses import dataclass, field
import heapq

from .clock import VirtualClock
from .faults import FaultManager
from .scenario import InjectionSpec, Scenario
from .service_chain import CallResult, ServiceChain


@dataclass
class AdaptiveObservation:
    request_id: int
    branch_id: str
    group: str
    scheduled_at_ms: int
    available_at_ms: int
    result: dict
    active_intervention_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "branch_id": self.branch_id,
            "group": self.group,
            "scheduled_at_ms": self.scheduled_at_ms,
            "available_at_ms": self.available_at_ms,
            "result": self.result,
            "active_intervention_ids": list(self.active_intervention_ids),
        }


class AdaptiveEngine:
    """运行一个独立自适应分支；迟到观察不会进入先前窗口。"""

    def __init__(self, chain: ServiceChain, manager_factory) -> None:
        self.chain = chain
        self.manager_factory = manager_factory

    def run_branch(
        self,
        scenario: Scenario,
        thresholds: dict,
        branch_id: str = "root",
        group_name: str = "adaptive_treatment",
        horizon_ms: int | None = None,
    ) -> dict:
        policy = scenario.adaptive_policy
        if not policy:
            raise ValueError("场景未配置 adaptive_policy")
        decision_times = [int(t) for t in policy["decision_at_ms"]]
        horizon = horizon_ms if horizon_ms is not None else self._horizon(
            scenario, decision_times)

        control = self._run_control(scenario, branch_id)
        (treatment, decisions, stopped, stop_reason, fault_events,
         suggestions, approvals) = self._run_treatment(
            scenario, thresholds, decision_times, horizon, branch_id, group_name)
        control_by_id = {o.request_id: o for o in control}
        comparisons = [self._compare(control_by_id[o.request_id], o)
                       for o in treatment]
        return {
            "branch_id": branch_id,
            "parent_id": None if branch_id == "root" else "root",
            "group": group_name,
            "thresholds": dict(thresholds),
            "control_observations": [o.to_dict() for o in control],
            "observations": [o.to_dict() for o in treatment],
            "comparisons": comparisons,
            "decisions": decisions,
            "suggestions": suggestions,
            "approvals": approvals,
            "explanations": self._explain(control, treatment),
            "fault_events": fault_events,
            "stopped": stopped,
            "stop_reason": stop_reason,
        }

    def _run_control(self, scenario: Scenario, branch_id: str):
        observations = []
        for request_id, request in enumerate(scenario.requests):
            result = self.chain.call(request_id, request.payload, scenario.seed, None)
            observations.append(self._observation(
                request_id, branch_id, "control", request.at_ms, result, []))
        return observations

    def _run_treatment(self, scenario, thresholds, decision_times, horizon_ms,
                       branch_id, group_name):
        policy = scenario.adaptive_policy
        approval_cfg = policy.get("approval", {})
        approval_required = bool(approval_cfg.get("required", False))
        overlap_mode = approval_cfg.get("overlap", "wait")
        expires_after = approval_cfg.get("expires_after_ms")
        clock = VirtualClock()
        manager = self.manager_factory()
        queue: list[tuple] = []
        counter = 0

        def push(at_ms, order, kind, payload=None):
            nonlocal counter
            heapq.heappush(queue, (at_ms, order, counter, kind, payload or {}))
            counter += 1

        for idx, request in enumerate(scenario.requests):
            push(request.at_ms, 2, "request_start",
                 {"request_id": idx, "payload": request.payload})
        for spec in scenario.injections:
            push(spec.start_ms, 0, "inject_start", {"spec": spec})
            if spec.end_ms is not None:
                push(spec.end_ms, 1, "inject_end", {"id": spec.id})
        for idx, at_ms in enumerate(decision_times):
            push(at_ms, 1, "decision", {"index": idx})
        for entry in approval_cfg.get("decisions", []):
            push(int(entry["at_ms"]), 1, "approval", {"entry": entry})
        if scenario.cancel_at_ms is not None:
            push(scenario.cancel_at_ms, 1, "cancel")
        push(horizon_ms, 2, "close")

        observations: list[AdaptiveObservation] = []
        decisions: list[dict] = []
        suggestions: dict[str, dict] = {}
        approvals: list[dict] = []
        escalated_failures: set[int] = set()
        spent_cost = 0.0
        spent_risk = 0.0
        intervention_count = 0
        stopped = False
        stop_reason = None
        cancelled = False
        closed = False
        pending_request_starts = len(scenario.requests)
        active_requests = 0
        pending_decisions = len(decision_times)
        scheduled_ends: list[int] = []
        active_open_effect_ids: set[str] = set()
        finish_scheduled = False

        def schedule_finish_if_complete():
            nonlocal finish_scheduled
            if ((cancelled or closed)
                    and pending_request_starts == 0 and active_requests == 0
                    and pending_decisions == 0
                    and not active_open_effect_ids
                    and not finish_scheduled):
                finish_at = max([horizon_ms, clock.now_ms()] + scheduled_ends,
                                default=horizon_ms)
                push(finish_at, 3, "finish")
                finish_scheduled = True

        while queue:
            at_ms, order, _, kind, payload = heapq.heappop(queue)
            clock.advance_to(VirtualClock.ms_to_ns(at_ms))

            if closed and kind in ("request_start", "decision"):
                if kind == "request_start":
                    pending_request_starts -= 1
                else:
                    pending_decisions -= 1
            elif kind == "request_start":
                pending_request_starts -= 1
                active_requests += 1
                active_before = manager.active_ids()
                result = self.chain.call(
                    payload["request_id"], payload["payload"], scenario.seed, manager)
                available_at = at_ms + result.total_latency_ns // 1_000_000
                observation = self._observation(
                    payload["request_id"], branch_id, group_name, at_ms,
                    result, active_before)
                push(available_at, 0, "observation_available",
                     {"observation": observation})
            elif kind == "inject_start":
                manager.start(payload["spec"], at_ms)
                if payload["spec"].end_ms is not None:
                    scheduled_ends.append(payload["spec"].end_ms)
                else:
                    active_open_effect_ids.add(payload["spec"].id)
            elif kind == "observation_available":
                observations.append(payload["observation"])
                active_requests -= 1
                schedule_finish_if_complete()
            elif kind == "decision" and not cancelled and not closed:
                pending_decisions -= 1
                decision, escalated_failures, spent_cost, spent_risk, intervention_count, \
                    became_stopped, current_stop_reason, spec = self._decide(
                        scenario, thresholds, observations, payload["index"], at_ms,
                        horizon_ms, branch_id, escalated_failures, spent_cost,
                        spent_risk, intervention_count, stopped, stop_reason,
                        defer_budget=approval_required)
                decisions.append(decision)
                if became_stopped:
                    stopped = True
                    stop_reason = current_stop_reason
                if spec is not None and approval_required:
                    suggestions[spec.id] = self._new_suggestion(
                        decision, spec, manager, thresholds, at_ms,
                        expires_after)
                elif spec is not None and decision["action"]["selected"]:
                    manager.start(spec, at_ms)
                    if spec.end_ms is not None:
                        push(spec.end_ms, 1, "inject_end", {"id": spec.id})
                        scheduled_ends.append(spec.end_ms)
                    else:
                        active_open_effect_ids.add(spec.id)
                schedule_finish_if_complete()
            elif kind == "approval" and not cancelled and not closed:
                entry = payload["entry"]
                (spent_cost, spent_risk, intervention_count,
                 stopped, stop_reason) = self._handle_approval(
                    entry, at_ms, suggestions, approvals, manager,
                    policy, limits=policy.get("limits", {}),
                    overlap_mode=overlap_mode,
                    spent_cost=spent_cost, spent_risk=spent_risk,
                    intervention_count=intervention_count,
                    stopped=stopped, stop_reason=stop_reason,
                    observations=observations, horizon_ms=horizon_ms,
                    push=push, scheduled_ends=scheduled_ends,
                    active_open_effect_ids=active_open_effect_ids)
                schedule_finish_if_complete()
            elif kind == "approval":
                approvals.append({
                    "at_ms": at_ms,
                    "suggestion_id": payload["entry"].get("suggestion"),
                    "choice": payload["entry"].get("choice"),
                    "outcome": "rejected",
                    "reason": ("experiment_cancelled" if cancelled
                               else "horizon_closed"),
                    "recheck": None,
                })
                schedule_finish_if_complete()
            elif kind == "decision":
                pending_decisions -= 1
                decisions.append({
                    "index": payload["index"],
                    "at_ms": at_ms,
                    "severity": "not_evaluated",
                    "reason": ("experiment_cancelled" if cancelled
                               else "horizon_closed"),
                    "thresholds": dict(thresholds),
                    "action": None,
                })
                schedule_finish_if_complete()
            elif kind == "inject_end":
                manager.end(payload["id"], at_ms)
                active_open_effect_ids.discard(payload["id"])
                schedule_finish_if_complete()
            elif kind == "cancel":
                cancelled = True
                stopped = True
                stop_reason = "experiment_cancelled"
                pending_request_starts = 0
                pending_decisions = 0
                active_open_effect_ids.clear()
                manager.recover_all(at_ms, reason="experiment_cancelled")
            elif kind == "close":
                closed = True
                pending_request_starts = 0
                pending_decisions = 0
                schedule_finish_if_complete()
            elif kind == "finish":
                reason = "experiment_cancelled" if cancelled else "experiment_finished"
                manager.recover_all(at_ms, reason=reason)
            schedule_finish_if_complete()

        if not manager.is_empty():
            raise RuntimeError("演练结束后仍有故障未恢复")
        suggestion_list = self._finalize_suggestions(
            suggestions, observations, manager.history, clock.now_ms())
        return (observations, decisions, stopped, stop_reason, manager.history,
                suggestion_list, approvals)

    def _decide(self, scenario, thresholds, observations, decision_index, at_ms,
                horizon_ms, branch_id, escalated_failures, spent_cost, spent_risk,
                intervention_count, already_stopped=False, existing_stop_reason=None,
                defer_budget=False):
        window_start = at_ms - int(thresholds.get("window_ms", 0))
        window = [o for o in observations
                  if window_start <= o.available_at_ms <= at_ms]
        window_ids = sorted({o.request_id for o in window})
        arrived_ids = sorted({o.request_id for o in observations
                              if o.available_at_ms <= at_ms})
        failed = [o for o in window if o.result["status"] != "ok"]
        failed_ids = sorted({o.request_id for o in failed})
        already = sorted(escalated_failures.intersection(failed_ids))
        new_failures = sorted(set(failed_ids) - escalated_failures)
        samples = len(window_ids)
        min_samples = int(thresholds.get("min_observations", 1))
        severity = "undetermined" if samples < min_samples else "healthy"

        if samples >= min_samples and new_failures:
            failure_rate = len(new_failures) / samples
            severity = ("failed" if failure_rate >= float(
                thresholds.get("failure_rate", 1.0)) else "degraded")
        if samples >= min_samples:
            average_latency_ms = sum(o.result["total_latency_ns"] for o in window) \
                / len(window) / 1_000_000
            if average_latency_ms >= float(
                    thresholds.get("avg_latency_ms", float("inf"))):
                severity = "degraded"

        template = scenario.adaptive_policy["actions"].get(severity)
        action_dict = None
        spec = None
        local_stop_reason = existing_stop_reason if already_stopped else None
        budget_before = {"cost": spent_cost, "risk": spent_risk,
                         "interventions": intervention_count}

        if template is not None and severity in {"degraded", "failed"}:
            cost = float(template.get("cost", 0))
            risk = float(template.get("risk", 0))
            candidate = InjectionSpec(
                id=f"{branch_id}:d{decision_index}:{severity}",
                type_name=template["type"],
                target=template["target"],
                start_ms=at_ms,
                end_ms=self._action_end(at_ms, template, horizon_ms),
                params=template.get("params", {}),
            )
            action_dict = {
                "severity": severity,
                "spec": self._injection_to_dict(candidate),
                "cost": cost,
                "risk": risk,
                "selected": False,
            }
            limits = scenario.adaptive_policy.get("limits", {})
            max_count = limits.get("max_interventions")
            max_cost = limits.get("max_cost")
            max_risk = limits.get("max_risk")
            if already_stopped:
                pass
            elif max_count is not None and intervention_count >= int(max_count):
                local_stop_reason = "max_interventions_reached"
            elif max_cost is not None and spent_cost + cost > float(max_cost):
                local_stop_reason = "budget_exhausted"
            elif max_risk is not None and spent_risk + risk > float(max_risk):
                local_stop_reason = "risk_limit_reached"
            else:
                spec = candidate
                action_dict["selected"] = True
                if defer_budget:
                    # 需要人工确认：预算在确认时才真正扣除。
                    action_dict["requires_confirmation"] = True
                    action_dict["suggestion_id"] = candidate.id
                else:
                    spent_cost += cost
                    spent_risk += risk
                    intervention_count += 1
                escalated_failures.update(new_failures)
            if local_stop_reason:
                action_dict["skip_reason"] = local_stop_reason
                escalated_failures.update(new_failures)

        decision = {
            "index": decision_index,
            "at_ms": at_ms,
            "window_start_ms": window_start,
            "window_end_ms": at_ms,
            "thresholds": dict(thresholds),
            "severity": severity,
            "arrived_observation_ids": arrived_ids,
            "window_observation_ids": window_ids,
            "failed_observation_ids": failed_ids,
            "new_failure_ids": new_failures,
            "already_escalated_failure_ids": already,
            "action": action_dict,
            "budget_before": budget_before,
            "budget_after": {"cost": spent_cost, "risk": spent_risk,
                             "interventions": intervention_count},
        }
        return (decision, escalated_failures, spent_cost, spent_risk,
                intervention_count, local_stop_reason is not None,
                local_stop_reason, spec)

    @staticmethod
    def _action_end(at_ms, template, horizon_ms):
        if template.get("end_ms") is not None:
            return min(horizon_ms, int(template["end_ms"]))
        duration_ms = template.get("duration_ms")
        return None if duration_ms is None else min(horizon_ms, at_ms + int(duration_ms))

    def _new_suggestion(self, decision, spec, manager, thresholds, at_ms,
                        expires_after):
        expires_at = None if expires_after is None else at_ms + int(expires_after)
        return {
            "id": spec.id,
            "severity": decision["severity"],
            "spec": self._injection_to_dict(spec),
            "suggested_at_ms": at_ms,
            "expires_at_ms": expires_at,
            "status": "pending_confirmation",
            # 确认依据快照：迟到观察只会进入 recheck，不会改写这里。
            "basis": {
                "window_observation_ids": decision["window_observation_ids"],
                "new_failure_ids": decision["new_failure_ids"],
                "arrived_observation_ids": decision["arrived_observation_ids"],
                "active_intervention_ids": manager.active_ids(),
                "budget_before": decision["budget_before"],
                "thresholds": dict(thresholds),
            },
            "confirmed_at_ms": None,
            "effective_spec": None,
            "withdrawn_at_ms": None,
        }

    def _handle_approval(self, entry, at_ms, suggestions, approvals, manager,
                         policy, limits, overlap_mode, spent_cost, spent_risk,
                         intervention_count, stopped, stop_reason, observations,
                         horizon_ms, push, scheduled_ends,
                         active_open_effect_ids):
        suggestion_id = entry.get("suggestion")
        choice = entry.get("choice")
        record = {"at_ms": at_ms, "suggestion_id": suggestion_id,
                  "choice": choice, "outcome": None, "reason": None,
                  "recheck": None}
        suggestion = suggestions.get(suggestion_id)

        if suggestion is None:
            record.update(outcome="rejected", reason="unknown_suggestion")
        elif choice == "abandon":
            # 放弃尚未生效的建议：不改变任何历史观察与预算。
            if suggestion["status"] != "pending_confirmation":
                record.update(outcome="rejected", reason="not_pending")
            else:
                suggestion["status"] = "abandoned"
                record["outcome"] = "abandoned"
        elif choice == "confirm":
            (spent_cost, spent_risk, intervention_count, stopped, stop_reason,
             record) = self._confirm(
                suggestion, record, at_ms, manager, policy, limits,
                overlap_mode, spent_cost, spent_risk, intervention_count,
                stopped, stop_reason, observations, horizon_ms, push,
                scheduled_ends, active_open_effect_ids)
        elif choice == "withdraw":
            # 撤回已生效干预：按原有收尾约定立即撤销，并记录撤回时刻。
            if suggestion["status"] not in ("confirmed",) \
                    or suggestion_id not in manager.active_ids():
                record.update(outcome="rejected", reason="not_effective")
            else:
                manager.end(suggestion_id, at_ms)
                active_open_effect_ids.discard(suggestion_id)
                suggestion["status"] = "withdrawn"
                suggestion["withdrawn_at_ms"] = at_ms
                record["outcome"] = "withdrawn"
        else:
            record.update(outcome="rejected", reason="unknown_choice")

        approvals.append(record)
        return spent_cost, spent_risk, intervention_count, stopped, stop_reason

    def _confirm(self, suggestion, record, at_ms, manager, policy, limits,
                 overlap_mode, spent_cost, spent_risk, intervention_count,
                 stopped, stop_reason, observations, horizon_ms, push,
                 scheduled_ends, active_open_effect_ids):
        template = policy["actions"][suggestion["severity"]]
        candidate = InjectionSpec(
            id=suggestion["id"],
            type_name=template["type"],
            target=template["target"],
            start_ms=at_ms,
            end_ms=self._action_end(at_ms, template, horizon_ms),
            params=template.get("params", {}),
        )
        conflicts = manager.conflicts(candidate)
        # 确认时的现场重检：风险余额、观察范围、仍在生效的干预。
        record["recheck"] = {
            "arrived_observation_ids": sorted(
                {o.request_id for o in observations
                 if o.available_at_ms <= at_ms}),
            "active_intervention_ids": manager.active_ids(),
            "budget": {"cost": spent_cost, "risk": spent_risk,
                       "interventions": intervention_count},
            "conflicting_intervention_ids": conflicts,
            "can_coexist": not conflicts or overlap_mode == "stack",
            "basis_intact": True,
        }

        reason = None
        if suggestion["status"] != "pending_confirmation":
            reason = "not_pending"
        elif (suggestion["expires_at_ms"] is not None
                and at_ms > suggestion["expires_at_ms"]):
            suggestion["status"] = "expired"
            reason = "suggestion_expired"
        elif stopped:
            # 已触发的停止条件不能被过期/新确认绕过。
            reason = "stop_condition_active"
        else:
            cost = float(template.get("cost", 0))
            risk = float(template.get("risk", 0))
            max_count = limits.get("max_interventions")
            max_cost = limits.get("max_cost")
            max_risk = limits.get("max_risk")
            if max_count is not None and intervention_count >= int(max_count):
                reason = "max_interventions_reached"
            elif max_cost is not None and spent_cost + cost > float(max_cost):
                reason = "budget_exhausted"
            elif max_risk is not None and spent_risk + risk > float(max_risk):
                reason = "risk_limit_reached"
            elif conflicts and overlap_mode == "wait":
                # 争用同一范围：必须等待前一项结束，建议保持待确认。
                record.update(outcome="rejected", reason="scope_contention")
                return (spent_cost, spent_risk, intervention_count,
                        stopped, stop_reason, record)
            else:
                manager.start(candidate, at_ms)
                if candidate.end_ms is not None:
                    push(candidate.end_ms, 1, "inject_end",
                         {"id": candidate.id})
                    scheduled_ends.append(candidate.end_ms)
                else:
                    active_open_effect_ids.add(candidate.id)
                spent_cost += cost
                spent_risk += risk
                intervention_count += 1
                suggestion["status"] = "confirmed"
                suggestion["confirmed_at_ms"] = at_ms
                suggestion["effective_spec"] = self._injection_to_dict(candidate)
                record["outcome"] = "confirmed"
                return (spent_cost, spent_risk, intervention_count,
                        stopped, stop_reason, record)

        record.update(outcome="rejected", reason=reason)
        if reason in ("max_interventions_reached", "budget_exhausted",
                      "risk_limit_reached"):
            stopped = True
            stop_reason = reason
        return spent_cost, spent_risk, intervention_count, stopped, stop_reason, record

    @staticmethod
    def _finalize_suggestions(suggestions, observations, fault_history,
                              final_ms):
        ended_ids = {e["id"] for e in fault_history
                     if e["event"] in ("inject_end", "recover")}
        result = []
        for suggestion in suggestions.values():
            status = suggestion["status"]
            if status == "pending_confirmation":
                expires = suggestion["expires_at_ms"]
                status = ("expired" if expires is not None and expires < final_ms
                          else "unconfirmed")
            elif status == "confirmed" and suggestion["id"] in ended_ids:
                status = "completed"
            suggestion["status"] = status
            if status in ("confirmed", "completed", "withdrawn"):
                suggestion["affected_observation_ids"] = [
                    o.request_id for o in observations
                    if suggestion["id"] in o.active_intervention_ids
                ]
            if status == "withdrawn":
                # 撤回后的到达结果都受这次收尾影响，逐条记录。
                suggestion["post_withdrawal_observation_ids"] = [
                    o.request_id for o in observations
                    if o.available_at_ms >= suggestion["withdrawn_at_ms"]
                ]
            result.append(suggestion)
        return result

    @staticmethod
    def _horizon(scenario, decision_times):
        candidates = [r.at_ms for r in scenario.requests] + decision_times
        if scenario.cancel_at_ms is not None:
            candidates.append(scenario.cancel_at_ms)
        approval = scenario.adaptive_policy.get("approval", {})
        candidates += [int(e["at_ms"]) for e in approval.get("decisions", [])]
        return max(candidates, default=0)

    def _observation(self, request_id, branch_id, group, scheduled_at,
                     result: CallResult, active_ids):
        return AdaptiveObservation(
            request_id=request_id,
            branch_id=branch_id,
            group=group,
            scheduled_at_ms=scheduled_at,
            available_at_ms=scheduled_at + result.total_latency_ns // 1_000_000,
            result=result.to_dict(),
            active_intervention_ids=list(active_ids),
        )

    @staticmethod
    def _injection_to_dict(spec: InjectionSpec) -> dict:
        return {
            "id": spec.id,
            "type": spec.type_name,
            "target": spec.target,
            "start_ms": spec.start_ms,
            "end_ms": spec.end_ms,
            "params": spec.params,
        }

    @staticmethod
    def _compare(control: AdaptiveObservation, treatment: AdaptiveObservation) -> dict:
        return {
            "request_id": treatment.request_id,
            "at_ms": treatment.scheduled_at_ms,
            "available_at_ms": treatment.available_at_ms,
            "status_changed": control.result["status"] != treatment.result["status"],
            "control_status": control.result["status"],
            "treatment_status": treatment.result["status"],
            "latency_delta_ns": treatment.result["total_latency_ns"]
            - control.result["total_latency_ns"],
            "failed_node": treatment.result.get("failed_node"),
            "active_intervention_ids": treatment.active_intervention_ids,
        }

    @staticmethod
    def _explain(control, treatment):
        control_by_id = {o.request_id: o for o in control}
        explanations = []
        for observed in treatment:
            baseline = control_by_id[observed.request_id]
            changed = baseline.result != observed.result
            if changed and observed.active_intervention_ids:
                source = "intervention"
            elif changed:
                source = "system_change"
            elif baseline.result["status"] != "ok":
                source = "natural_system_failure"
            else:
                source = "control_baseline"
            explanations.append({
                "request_id": observed.request_id,
                "at_ms": observed.scheduled_at_ms,
                "source": source,
                "active_intervention_ids": observed.active_intervention_ids,
                "control_status": baseline.result["status"],
                "observed_status": observed.result["status"],
            })
        return explanations
