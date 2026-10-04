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
        approvals: list[dict] | None = None,
    ) -> dict:
        policy = scenario.adaptive_policy
        if not policy:
            raise ValueError("场景未配置 adaptive_policy")
        decision_times = [int(t) for t in policy["decision_at_ms"]]
        horizon = horizon_ms if horizon_ms is not None else self._horizon(
            scenario, decision_times)
        if approvals:
            horizon = max(horizon, max(int(a["at_ms"]) for a in approvals))

        control = self._run_control(scenario, branch_id)
        treatment, decisions, stopped, stop_reason, fault_events, suggestions, groups = \
            self._run_treatment(scenario, thresholds, decision_times, horizon,
                                branch_id, group_name, approvals or [])
        control_by_id = {o.request_id: o for o in control}
        comparisons = [self._compare(control_by_id[o.request_id], o)
                       for o in treatment]
        # 记录每次实际生效干预影响了哪些后续观察。
        for suggestion in suggestions:
            suggestion["affected_observation_ids"] = [
                o.request_id for o in treatment
                if suggestion["id"] in o.active_intervention_ids
            ]
        return {
            "branch_id": branch_id,
            "parent_id": None if branch_id == "root" else "root",
            "group": group_name,
            "thresholds": dict(thresholds),
            "approvals": [dict(a) for a in (approvals or [])],
            "suggestions": suggestions,
            "groups": groups,
            "control_observations": [o.to_dict() for o in control],
            "observations": [o.to_dict() for o in treatment],
            "comparisons": comparisons,
            "decisions": decisions,
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
                       branch_id, group_name, approvals):
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
        for event in approvals:
            push(int(event["at_ms"]), 1, "approval", {"event": event})
        if scenario.cancel_at_ms is not None:
            push(scenario.cancel_at_ms, 1, "cancel")
        push(horizon_ms, 2, "close")

        policy = scenario.adaptive_policy
        approval_cfg = policy.get("approval", {})
        approval_required = bool(approval_cfg.get("required", False))
        allow_overlap = bool(approval_cfg.get("allow_overlap", False))
        expires_after_ms = approval_cfg.get("expires_after_ms")
        limits = policy.get("limits", {})
        group_configs = {g["id"]: g for g in approval_cfg.get("groups", [])}
        decision_to_group: dict[int, str] = {}
        for group_cfg in group_configs.values():
            for decision_idx in group_cfg.get("decision_indices", []):
                decision_to_group[int(decision_idx)] = group_cfg["id"]
        groups: list[dict] = []
        group_by_id: dict[str, dict] = {}
        for group_cfg in group_configs.values():
            record = {
                "id": group_cfg["id"],
                "mode": group_cfg.get("mode", "all_of"),
                "status": "open",
                "member_ids": [],
                "budget": dict(group_cfg.get("budget", {})),
                "spent": {"cost": 0.0, "risk": 0.0, "interventions": 0},
                "events": [],
            }
            groups.append(record)
            group_by_id[record["id"]] = record
        observations: list[AdaptiveObservation] = []
        decisions: list[dict] = []
        suggestions: list[dict] = []
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

        def expire_pending(now_ms):
            for suggestion in suggestions:
                valid_until = suggestion["valid_until_ms"]
                if (suggestion["status"] == "pending"
                        and valid_until is not None and valid_until < now_ms):
                    suggestion["status"] = "expired"
                    suggestion["expired_at_ms"] = valid_until

        def find_suggestion(event):
            suggestion_id = event.get("suggestion_id")
            if suggestion_id is not None:
                for suggestion in suggestions:
                    if suggestion["id"] == suggestion_id:
                        return suggestion
                return None
            decision_index = event.get("decision_index")
            matches = [s for s in suggestions
                       if s["decision_index"] == decision_index]
            return matches[-1] if matches else None

        def group_members(group_id):
            return [s for s in suggestions if s.get("group_id") == group_id]

        def build_candidate(suggestion, at_ms):
            return InjectionSpec(
                id=suggestion["id"],
                type_name=suggestion["spec"]["type"],
                target=suggestion["spec"]["target"],
                start_ms=at_ms,
                end_ms=self._action_end(at_ms, suggestion["template"],
                                        horizon_ms),
                params=suggestion["spec"]["params"],
            )

        def recheck_suggestion(suggestion, at_ms, planned):
            """执行前复核：有效期、停止状态、全局限额、组合预算、范围争用。

            planned 是本次组合确认中已排定执行成员的累计
            (cost, risk, count)，保证整组确认不超组合预算。
            """
            if suggestion["status"] != "pending":
                return f"suggestion_{suggestion['status']}"
            if cancelled:
                return "experiment_cancelled"
            if closed or stopped:
                return "stop_condition_active"
            plan_cost, plan_risk, plan_count = planned
            max_count = limits.get("max_interventions")
            max_cost = limits.get("max_cost")
            max_risk = limits.get("max_risk")
            if (max_count is not None
                    and intervention_count + plan_count >= int(max_count)):
                return "max_interventions_reached"
            if (max_cost is not None and spent_cost + plan_cost
                    + suggestion["cost"] > float(max_cost)):
                return "budget_exhausted"
            if (max_risk is not None and spent_risk + plan_risk
                    + suggestion["risk"] > float(max_risk)):
                return "risk_limit_reached"
            group_id = suggestion.get("group_id")
            if group_id is not None:
                record = group_by_id[group_id]
                budget = record["budget"]
                spent = record["spent"]
                if (budget.get("max_interventions") is not None
                        and spent["interventions"] + plan_count
                        >= int(budget["max_interventions"])):
                    return "group_budget_exhausted"
                if (budget.get("max_cost") is not None
                        and spent["cost"] + plan_cost + suggestion["cost"]
                        > float(budget["max_cost"])):
                    return "group_budget_exhausted"
                if (budget.get("max_risk") is not None
                        and spent["risk"] + plan_risk + suggestion["risk"]
                        > float(budget["max_risk"])):
                    return "group_budget_exhausted"
            if not allow_overlap:
                candidate = build_candidate(suggestion, at_ms)
                if manager.conflicting_ids(candidate):
                    return "scope_contention"
            return None

        def execute_suggestion(suggestion, at_ms):
            nonlocal spent_cost, spent_risk, intervention_count
            candidate = build_candidate(suggestion, at_ms)
            manager.start(candidate, at_ms)
            spent_cost += suggestion["cost"]
            spent_risk += suggestion["risk"]
            intervention_count += 1
            group_id = suggestion.get("group_id")
            if group_id is not None:
                spent = group_by_id[group_id]["spent"]
                spent["cost"] += suggestion["cost"]
                spent["risk"] += suggestion["risk"]
                spent["interventions"] += 1
            suggestion["status"] = "effective"
            suggestion["effective_at_ms"] = at_ms
            if candidate.end_ms is not None:
                push(candidate.end_ms, 1, "inject_end", {"id": candidate.id})
                scheduled_ends.append(candidate.end_ms)
            else:
                active_open_effect_ids.add(candidate.id)

        limit_reasons = {"max_interventions_reached", "budget_exhausted",
                         "risk_limit_reached", "group_budget_exhausted"}

        def handle_group_event(event, at_ms):
            """组合确认/放弃：确认按成员快照核对，条件变化则整组拒绝。"""
            nonlocal stopped, stop_reason
            group_id = event.get("group_id")
            action = event.get("action", "confirm")
            record = group_by_id.get(group_id)
            if record is None:
                return
            members = group_members(group_id)
            group_event = {"at_ms": at_ms, "action": action,
                           "group_id": group_id}
            if action == "abandon":
                abandoned = []
                for suggestion in members:
                    if suggestion["status"] == "pending":
                        suggestion["status"] = "abandoned"
                        suggestion["abandoned_at_ms"] = at_ms
                        abandoned.append(suggestion["id"])
                record["status"] = ("partially_effective"
                                    if record["spent"]["interventions"]
                                    else "abandoned")
                group_event.update({"outcome": "abandoned",
                                    "abandoned_ids": abandoned})
            elif action == "confirm":
                expected = event.get("members")
                expected_ids = (sorted(str(m) for m in expected)
                                if expected is not None
                                else sorted(s["id"] for s in members))
                confirmable = sorted(s["id"] for s in members
                                     if s["status"] == "pending")
                if record["mode"] == "all_of":
                    # all_of：确认快照必须与当前全部待确认成员一致。
                    conditions_ok = confirmable == expected_ids
                else:
                    # any_of：允许选择子集，但选中项必须全部仍然有效。
                    conditions_ok = all(i in confirmable
                                        for i in expected_ids)
                if not conditions_ok:
                    # 针对旧条件的整组确认不能套用到变化后的剩余建议。
                    group_event.update({
                        "outcome": "rejected",
                        "reason": "conditions_changed",
                        "expected_ids": expected_ids,
                        "confirmable_ids": confirmable,
                    })
                else:
                    planned = [0.0, 0.0, 0]
                    results = []
                    for suggestion in members:
                        if (suggestion["status"] != "pending"
                                or suggestion["id"] not in expected_ids):
                            continue
                        reason = recheck_suggestion(suggestion, at_ms, planned)
                        if reason is None:
                            planned[0] += suggestion["cost"]
                            planned[1] += suggestion["risk"]
                            planned[2] += 1
                        elif reason in limit_reasons and not stopped:
                            stopped = True
                            stop_reason = reason
                        results.append((suggestion, reason))
                    failed = [r for _, r in results if r is not None]
                    if record["mode"] == "all_of" and failed:
                        # all_of 原子语义：任一复核失败则整组不执行。
                        for suggestion, reason in results:
                            suggestion["rechecks"].append({
                                "at_ms": at_ms, "group_id": group_id,
                                "outcome": "blocked",
                                "reason": reason or "group_recheck_failed",
                            })
                        group_event.update({
                            "outcome": "rejected",
                            "reason": "group_recheck_failed",
                            "failed_ids": [s["id"] for s, r in results
                                           if r is not None],
                        })
                    else:
                        executed = []
                        for suggestion, reason in results:
                            if reason is None:
                                execute_suggestion(suggestion, at_ms)
                                executed.append(suggestion["id"])
                                suggestion["rechecks"].append({
                                    "at_ms": at_ms, "group_id": group_id,
                                    "outcome": "passed",
                                })
                            else:
                                # 失效/超限部分保持待确认，不自动换成别的动作。
                                suggestion["rechecks"].append({
                                    "at_ms": at_ms, "group_id": group_id,
                                    "outcome": "skipped",
                                    "reason": reason,
                                })
                        total = len(record["member_ids"])
                        done = record["spent"]["interventions"]
                        record["status"] = ("effective" if done == total
                                            else "partially_effective"
                                            if done else "open")
                        group_event.update({
                            "outcome": ("confirmed" if len(executed)
                                        == len(results)
                                        else "partially_confirmed"),
                            "executed_ids": executed,
                            "skipped_ids": [s["id"] for s, r in results
                                            if r is not None],
                            "budget_after": {
                                "cost": spent_cost,
                                "risk": spent_risk,
                                "interventions": intervention_count,
                            },
                            "group_spent": dict(record["spent"]),
                        })
            else:
                group_event.update({"outcome": "ignored",
                                    "reason": "unknown_action"})
            record["events"].append(group_event)

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
            expire_pending(at_ms)

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
                observation = payload["observation"]
                observations.append(observation)
                # 组合成员在等待确认期间被新观察推翻依据：标记失效，
                # 失效部分不能被确认，也不会被自动替换成另一个动作。
                for suggestion in suggestions:
                    group_id = suggestion.get("group_id")
                    if (suggestion["status"] == "pending"
                            and group_id is not None
                            and group_configs[group_id].get(
                                "invalidate_on_new_observation", True)
                            and at_ms > suggestion["suggested_at_ms"]):
                        suggestion["status"] = "invalidated"
                        suggestion["invalidated_at_ms"] = at_ms
                        suggestion["invalidated_by_observation_id"] = \
                            observation.request_id
                        suggestion["invalidation_reason"] = "basis_superseded"
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
                if spec is not None and decision["action"]["selected"]:
                    if approval_required:
                        # 仅生成建议，等待人工确认后才实际生效。
                        template = scenario.adaptive_policy["actions"][
                            decision["severity"]]
                        suggestion = {
                            "id": spec.id,
                            "decision_index": payload["index"],
                            "suggested_at_ms": at_ms,
                            "valid_until_ms": (
                                None if expires_after_ms is None
                                else at_ms + int(expires_after_ms)),
                            "severity": decision["severity"],
                            "basis": {
                                "window_observation_ids":
                                    decision["window_observation_ids"],
                                "arrived_observation_ids":
                                    decision["arrived_observation_ids"],
                                "new_failure_ids": decision["new_failure_ids"],
                            },
                            "spec": self._injection_to_dict(spec),
                            "cost": float(template.get("cost", 0)),
                            "risk": float(template.get("risk", 0)),
                            "template": dict(template),
                            "status": "pending",
                            "events": [],
                            "rechecks": [],
                            "group_id": decision_to_group.get(payload["index"]),
                        }
                        suggestions.append(suggestion)
                        if suggestion["group_id"] is not None:
                            group_by_id[suggestion["group_id"]][
                                "member_ids"].append(suggestion["id"])
                        decision["action"]["suggestion_id"] = suggestion["id"]
                        decision["action"]["approval"] = "pending"
                    else:
                        manager.start(spec, at_ms)
                        if spec.end_ms is not None:
                            push(spec.end_ms, 1, "inject_end", {"id": spec.id})
                            scheduled_ends.append(spec.end_ms)
                        else:
                            active_open_effect_ids.add(spec.id)
                schedule_finish_if_complete()
            elif kind == "approval":
                event = payload["event"]
                action = event.get("action", "confirm")
                if event.get("group_id") is not None:
                    handle_group_event(event, at_ms)
                    schedule_finish_if_complete()
                    continue
                suggestion = find_suggestion(event)
                record = {
                    "at_ms": at_ms,
                    "action": action,
                    "suggestion_id": None if suggestion is None else suggestion["id"],
                }
                if suggestion is None:
                    record.update({"outcome": "ignored",
                                   "reason": "unknown_suggestion"})
                elif action == "confirm":
                    if suggestion["status"] != "pending":
                        record.update({"outcome": "rejected",
                                       "reason": f"suggestion_{suggestion['status']}"})
                    elif cancelled:
                        record.update({"outcome": "rejected",
                                       "reason": "experiment_cancelled"})
                    elif closed or stopped:
                        # 已触发的停止条件不能被过期/迟到确认绕过。
                        record.update({"outcome": "rejected",
                                       "reason": "stop_condition_active"})
                    else:
                        # 确认时按当时的风险余额重新检查。
                        check_reason = None
                        max_count = limits.get("max_interventions")
                        max_cost = limits.get("max_cost")
                        max_risk = limits.get("max_risk")
                        if (max_count is not None
                                and intervention_count >= int(max_count)):
                            check_reason = "max_interventions_reached"
                        elif (max_cost is not None and spent_cost
                                + suggestion["cost"] > float(max_cost)):
                            check_reason = "budget_exhausted"
                        elif (max_risk is not None and spent_risk
                                + suggestion["risk"] > float(max_risk)):
                            check_reason = "risk_limit_reached"
                        if check_reason is not None:
                            stopped = True
                            stop_reason = check_reason
                            record.update({"outcome": "rejected",
                                           "reason": check_reason})
                        else:
                            candidate = InjectionSpec(
                                id=suggestion["id"],
                                type_name=suggestion["spec"]["type"],
                                target=suggestion["spec"]["target"],
                                start_ms=at_ms,
                                end_ms=self._action_end(
                                    at_ms, suggestion["template"], horizon_ms),
                                params=suggestion["spec"]["params"],
                            )
                            conflicts = manager.conflicting_ids(candidate)
                            if conflicts and not allow_overlap:
                                # 范围被在生效干预占用：保持待确认，可稍后再试。
                                record.update({
                                    "outcome": "deferred",
                                    "reason": "scope_contention",
                                    "conflicting_intervention_ids": conflicts,
                                })
                            else:
                                manager.start(candidate, at_ms)
                                spent_cost += suggestion["cost"]
                                spent_risk += suggestion["risk"]
                                intervention_count += 1
                                suggestion["status"] = "effective"
                                suggestion["effective_at_ms"] = at_ms
                                if candidate.end_ms is not None:
                                    push(candidate.end_ms, 1, "inject_end",
                                         {"id": candidate.id})
                                    scheduled_ends.append(candidate.end_ms)
                                else:
                                    active_open_effect_ids.add(candidate.id)
                                record.update({
                                    "outcome": "confirmed",
                                    "effective_at_ms": at_ms,
                                    "budget_after": {
                                        "cost": spent_cost,
                                        "risk": spent_risk,
                                        "interventions": intervention_count,
                                    },
                                })
                elif action == "abandon":
                    if suggestion["status"] != "pending":
                        record.update({"outcome": "rejected",
                                       "reason": f"suggestion_{suggestion['status']}"})
                    else:
                        # 放弃尚未生效的建议，不改写任何历史观察。
                        suggestion["status"] = "abandoned"
                        suggestion["abandoned_at_ms"] = at_ms
                        record.update({"outcome": "abandoned"})
                elif action == "withdraw":
                    if suggestion["status"] != "effective":
                        record.update({"outcome": "rejected",
                                       "reason": f"suggestion_{suggestion['status']}"})
                    else:
                        # 撤回已生效干预：按原有收尾约定撤销效果。
                        manager.end(suggestion["id"], at_ms)
                        active_open_effect_ids.discard(suggestion["id"])
                        suggestion["status"] = "withdrawn"
                        suggestion["withdrawn_at_ms"] = at_ms
                        record.update({"outcome": "withdrawn"})
                else:
                    record.update({"outcome": "ignored",
                                   "reason": "unknown_action"})
                if suggestion is not None:
                    suggestion["events"].append(record)
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
                for suggestion in suggestions:
                    if suggestion["status"] == "pending":
                        suggestion["status"] = "expired"
                        suggestion["expired_at_ms"] = at_ms
                for record in groups:
                    if record["status"] == "open":
                        record["status"] = "closed"
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
        expire_pending(horizon_ms if horizon_ms is not None else 0)
        return (observations, decisions, stopped, stop_reason, manager.history,
                suggestions, groups)

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
                if not defer_budget:
                    # 需要人工确认时，预算在实际确认生效时才扣减。
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

    @staticmethod
    def _horizon(scenario, decision_times):
        candidates = [r.at_ms for r in scenario.requests] + decision_times
        if scenario.cancel_at_ms is not None:
            candidates.append(scenario.cancel_at_ms)
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
