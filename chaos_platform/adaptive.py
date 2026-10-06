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
        (treatment, decisions, stopped, stop_reason, fault_events, suggestions,
         bundle_runs) = self._run_treatment(
            scenario, thresholds, decision_times, horizon,
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
            "bundles": bundle_runs,
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
        observations: list[AdaptiveObservation] = []
        decisions: list[dict] = []
        suggestions: list[dict] = []
        bundle_runs: list[dict] = []
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

        def build_candidate(suggestion, at_ms):
            return InjectionSpec(
                id=suggestion["id"],
                type_name=suggestion["spec"]["type"],
                target=suggestion["spec"]["target"],
                start_ms=at_ms,
                end_ms=self._action_end(
                    at_ms, suggestion["template"], horizon_ms),
                params=suggestion["spec"]["params"],
            )

        def precheck_snapshot(suggestion, at_ms):
            """执行前复核：有效期、当前状态、范围占用、预算（调用方补预算结论）。"""
            return {
                "checked_at_ms": at_ms,
                "suggestion_status": suggestion["status"],
                "suggested_at_ms": suggestion["suggested_at_ms"],
                "valid_until_ms": suggestion["valid_until_ms"],
                "within_validity": (
                    suggestion["valid_until_ms"] is None
                    or at_ms <= suggestion["valid_until_ms"]),
                "budget_before": {
                    "cost": spent_cost,
                    "risk": spent_risk,
                    "interventions": intervention_count,
                },
            }

        def validate_member(suggestion, at_ms):
            """返回 (reason, conflicts, candidate)；reason 为 None 表示可执行。

            范围争用只检查在生效的外部干预；同一组合成员被视为共同批准，
            互不占范围（预算与有效期在调用方按组合整体复核）。
            """
            if cancelled:
                return "experiment_cancelled", [], None
            if closed or stopped:
                # 已触发的停止条件不能被迟到确认绕过。
                return "stop_condition_active", [], None
            if suggestion["status"] != "pending":
                return f"suggestion_{suggestion['status']}", [], None
            max_count = limits.get("max_interventions")
            max_cost = limits.get("max_cost")
            max_risk = limits.get("max_risk")
            if (max_count is not None
                    and intervention_count >= int(max_count)):
                return "max_interventions_reached", [], None
            if (max_cost is not None and spent_cost
                    + suggestion["cost"] > float(max_cost)):
                return "budget_exhausted", [], None
            if (max_risk is not None and spent_risk
                    + suggestion["risk"] > float(max_risk)):
                return "risk_limit_reached", [], None
            candidate = build_candidate(suggestion, at_ms)
            conflicts = list(manager.conflicting_ids(candidate))
            if conflicts and not allow_overlap:
                return "scope_contention", sorted(set(conflicts)), candidate
            return None, [], candidate

        def apply_member(suggestion, candidate, at_ms, risk_charge=None):
            nonlocal spent_cost, spent_risk, intervention_count
            manager.start(candidate, at_ms)
            spent_cost += suggestion["cost"]
            spent_risk += suggestion["risk"] if risk_charge is None else risk_charge
            intervention_count += 1
            suggestion["status"] = "effective"
            suggestion["effective_at_ms"] = at_ms
            if candidate.end_ms is not None:
                push(candidate.end_ms, 1, "inject_end", {"id": candidate.id})
                scheduled_ends.append(candidate.end_ms)
            else:
                active_open_effect_ids.add(candidate.id)

        def member_risk_sources(member):
            return dict(member.get("risk_sources") or {})

        def incremental_risk(member, seen_sources):
            """共享风险来源按并集记账：同一来源只计一次，不朴素相加。"""
            sources = member_risk_sources(member)
            if not sources:
                return member["risk"]
            return sum(amount for source, amount in sources.items()
                       if source not in seen_sources)

        def handle_bundle_event(event, action, at_ms):
            """处理组合批准/放弃/重开，返回组合不可解析时的忽略原因或 None。

            批准事件可携带组合级 budget（max_cost/max_risk）与 plan
            （original_order / lowest_risk）。执行每一项前按最新状态复核
            剩余候选的有效期、证据、互斥与预算；已生效成员的成本与影响
            永久计入 committed，不因后续失败、撤回或重排而归零。替代项
            只能来自事件列出的、各自具备独立有效批准的建议，过期许可
            不能沿用。all 模式任一成员复核失败则整组不执行；partial
            模式只执行通过复核的成员，已生效事实保留。
            """
            nonlocal stopped, stop_reason
            max_count = limits.get("max_interventions")
            max_cost = limits.get("max_cost")
            max_risk = limits.get("max_risk")
            by_id = {s["id"]: s for s in suggestions}
            resumed_from = None
            source_run = None
            carried_committed: list[dict] = []
            if action == "resume_bundle":
                source_id = event.get("bundle_id")
                source_run = next((r for r in bundle_runs
                                   if r["id"] == source_id), None)
                if source_run is None:
                    return "unknown_bundle"
                raw_ids = [m["suggestion_id"]
                           for m in source_run.get("remaining", [])]
                if not raw_ids:
                    return "no_remaining_members"
                resumed_from = source_id
                carried_committed = list(source_run.get("committed", []))
                # 人工停止可通过重开继续；其余停止条件不可绕过。
                if stopped and stop_reason == "manual_stop":
                    stopped = False
                    stop_reason = None
            else:
                raw_ids = event.get("suggestion_ids")
            if not raw_ids:
                return "missing_suggestion_ids"
            unknown = [sid for sid in raw_ids if sid not in by_id]
            if unknown:
                return f"unknown_suggestion:{unknown[0]}"
            if len(set(raw_ids)) != len(raw_ids):
                return "duplicate_suggestion_ids"
            mode = event.get("mode",
                             "partial" if action == "resume_bundle" else "all")
            plan_name = event.get("plan", "original_order")
            budget_cfg = event.get("budget")
            if budget_cfg is None and source_run is not None:
                budget_cfg = source_run.get("budget")
            budget_cfg = dict(budget_cfg or {})
            bundle_max_cost = budget_cfg.get("max_cost")
            bundle_max_risk = budget_cfg.get("max_risk")
            run = {
                "id": f"bundle@{at_ms}:{len(bundle_runs)}",
                "at_ms": at_ms,
                "action": action,
                "mode": mode,
                "plan": plan_name,
                "budget": dict(budget_cfg),
                "resumed_from": resumed_from,
                "suggestion_ids": list(raw_ids),
                "members": [],
            }
            ordered = [by_id[sid] for sid in raw_ids]
            if action == "abandon_bundle":
                run["status"] = "abandoned"
                for member in ordered:
                    member_record = {
                        "at_ms": at_ms,
                        "action": "abandon_bundle",
                        "bundle_id": run["id"],
                        "suggestion_id": member["id"],
                    }
                    if member["status"] != "pending":
                        member_record.update({
                            "outcome": "rejected",
                            "reason": f"suggestion_{member['status']}",
                        })
                    else:
                        member["status"] = "abandoned"
                        member["abandoned_at_ms"] = at_ms
                        member_record.update({"outcome": "abandoned"})
                    member["events"].append(member_record)
                    run["members"].append(member_record)
                bundle_runs.append(run)
                return None
            if mode not in ("all", "partial"):
                run["status"] = "rejected"
                run["reject_reason"] = "unknown_mode"
                bundle_runs.append(run)
                return "unknown_mode"
            if plan_name not in ("original_order", "lowest_risk"):
                run["status"] = "rejected"
                run["reject_reason"] = "unknown_plan"
                bundle_runs.append(run)
                return "unknown_plan"

            carried_cost = sum(c["cost"] for c in carried_committed)
            carried_risk = sum(c["risk"] for c in carried_committed)
            carried_sources = {src for c in carried_committed
                               for src in c.get("risk_sources", {})}
            carried_groups = {c["mutex_group"] for c in carried_committed
                              if c.get("mutex_group")}
            effective_groups = {s["mutex_group"] for s in suggestions
                                if s["status"] == "effective"
                                and s.get("mutex_group")}

            def plan_block_reason(member, seen_sources, groups, proj,
                                  proj_bundle_cost, proj_bundle_risk):
                """剩余候选复核：状态、有效期、证据、互斥、全局与组合预算。"""
                if member["status"] == "effective":
                    return "already_effective"
                if member["status"] != "pending":
                    return f"suggestion_{member['status']}"
                if (member["valid_until_ms"] is not None
                        and at_ms > member["valid_until_ms"]):
                    return "suggestion_expired"
                if not member["basis"].get("window_observation_ids"):
                    return "missing_evidence"
                group = member.get("mutex_group")
                if group and group in groups:
                    return "mutex_conflict"
                if (max_count is not None
                        and proj["interventions"] + 1 > int(max_count)):
                    return "max_interventions_reached"
                if (max_cost is not None and proj["cost"]
                        + member["cost"] > float(max_cost)):
                    return "budget_exhausted"
                inc = incremental_risk(member, seen_sources)
                if (max_risk is not None and proj["risk"]
                        + inc > float(max_risk)):
                    return "risk_limit_reached"
                if (bundle_max_cost is not None and proj_bundle_cost
                        + member["cost"] > float(bundle_max_cost)):
                    return "bundle_budget_exceeded"
                if (bundle_max_risk is not None and proj_bundle_risk
                        + inc > float(bundle_max_risk)):
                    return "bundle_budget_exceeded"
                return None

            def simulate_plan(name, plan_members):
                """对同一批剩余候选推演一个明确方案，不改动任何状态。"""
                seen = set(carried_sources)
                groups = set(carried_groups) | set(effective_groups)
                proj = {"cost": spent_cost, "risk": spent_risk,
                        "interventions": intervention_count}
                proj_bundle_cost = carried_cost
                proj_bundle_risk = carried_risk
                entries = []
                for member in plan_members:
                    reason = plan_block_reason(
                        member, seen, groups, proj,
                        proj_bundle_cost, proj_bundle_risk)
                    entry = {"suggestion_id": member["id"],
                             "outcome": reason or "executable"}
                    if reason:
                        entry["reason"] = reason
                    else:
                        inc = incremental_risk(member, seen)
                        entry["incremental_risk"] = inc
                        seen.update(member_risk_sources(member))
                        if member.get("mutex_group"):
                            groups.add(member["mutex_group"])
                        proj["cost"] += member["cost"]
                        proj["risk"] += inc
                        proj["interventions"] += 1
                        proj_bundle_cost += member["cost"]
                        proj_bundle_risk += inc
                    entries.append(entry)
                return {
                    "name": name,
                    "order": [m["id"] for m in plan_members],
                    "entries": entries,
                    "projected": {
                        "cost": proj["cost"],
                        "risk": proj["risk"],
                        "interventions": proj["interventions"],
                        "bundle_cost": proj_bundle_cost,
                        "bundle_risk": proj_bundle_risk,
                    },
                }

            def risk_then_original(member):
                return (member["risk"], raw_ids.index(member["id"]))

            low_risk_order = sorted(ordered, key=risk_then_original)
            # 两种明确方案都完整推演并保存，实际执行选定的方案。
            run["plan_comparison"] = {
                "plans": [simulate_plan("original_order", ordered),
                          simulate_plan("lowest_risk", low_risk_order)],
                "selected": plan_name,
            }
            plan_order = (low_risk_order if plan_name == "lowest_risk"
                          else list(ordered))

            executed_sources = set(carried_sources)
            executed_groups = set(carried_groups) | set(effective_groups)
            bundle_cost = carried_cost
            bundle_risk = carried_risk
            committed_new: list[dict] = []
            checks = []
            proj_cost = spent_cost
            proj_risk = spent_risk
            proj_count = intervention_count
            proj_bundle_cost = bundle_cost
            proj_bundle_risk = bundle_risk
            proj_sources = set(executed_sources)
            proj_groups = set(executed_groups)
            for member in plan_order:
                snapshot = precheck_snapshot(member, at_ms)
                if member["status"] == "effective":
                    # 重复完成通知：已生效成员不重复执行、不重复计费。
                    checks.append({"member": member, "snapshot": snapshot,
                                   "reason": "already_effective",
                                   "conflicts": [], "candidate": None})
                    continue
                reason, conflicts, candidate = validate_member(member, at_ms)
                if reason in ("max_interventions_reached",
                              "budget_exhausted", "risk_limit_reached"):
                    # 预算类结论按组合内投影支出重新判断（见下）。
                    reason, conflicts = None, []
                    candidate = build_candidate(member, at_ms)
                inc = incremental_risk(member, proj_sources)
                if reason is None:
                    if (max_count is not None
                            and proj_count + 1 > int(max_count)):
                        reason = "max_interventions_reached"
                    elif (max_cost is not None and proj_cost
                          + member["cost"] > float(max_cost)):
                        reason = "budget_exhausted"
                    elif (max_risk is not None and proj_risk
                          + inc > float(max_risk)):
                        reason = "risk_limit_reached"
                    elif not member["basis"].get("window_observation_ids"):
                        reason = "missing_evidence"
                    elif (member.get("mutex_group")
                          and member["mutex_group"] in proj_groups):
                        reason = "mutex_conflict"
                    elif (bundle_max_cost is not None and proj_bundle_cost
                          + member["cost"] > float(bundle_max_cost)):
                        reason = "bundle_budget_exceeded"
                    elif (bundle_max_risk is not None and proj_bundle_risk
                          + inc > float(bundle_max_risk)):
                        reason = "bundle_budget_exceeded"
                if reason is None:
                    # 投影：后续成员按本成员已执行后的支出复核。
                    proj_count += 1
                    proj_cost += member["cost"]
                    proj_risk += inc
                    proj_bundle_cost += member["cost"]
                    proj_bundle_risk += inc
                    proj_sources.update(member_risk_sources(member))
                    if member.get("mutex_group"):
                        proj_groups.add(member["mutex_group"])
                checks.append({"member": member, "snapshot": snapshot,
                               "reason": reason, "conflicts": conflicts,
                               "candidate": candidate})
            any_blocked = any(c["reason"] not in (None, "already_effective")
                              for c in checks)
            blocked_by_mode = mode == "all" and any_blocked
            effective_ids = []
            rejected_ids = []
            for check in checks:
                member = check["member"]
                if not blocked_by_mode:
                    # 逐项复核：快照与预算按该成员实际执行前的最新支出
                    # 重新计算，不能复用组合开始时的快照。
                    check["snapshot"] = precheck_snapshot(member, at_ms)
                if not blocked_by_mode and check["reason"] is None:
                    inc = incremental_risk(member, executed_sources)
                    if (max_count is not None
                            and intervention_count + 1 > int(max_count)):
                        check["reason"] = "max_interventions_reached"
                    elif (max_cost is not None and spent_cost
                          + member["cost"] > float(max_cost)):
                        check["reason"] = "budget_exhausted"
                    elif (max_risk is not None and spent_risk
                          + inc > float(max_risk)):
                        check["reason"] = "risk_limit_reached"
                    elif (bundle_max_cost is not None and bundle_cost
                          + member["cost"] > float(bundle_max_cost)):
                        check["reason"] = "bundle_budget_exceeded"
                    elif (bundle_max_risk is not None and bundle_risk
                          + inc > float(bundle_max_risk)):
                        check["reason"] = "bundle_budget_exceeded"
                member_record = {
                    "at_ms": at_ms,
                    "action": action,
                    "bundle_id": run["id"],
                    "mode": mode,
                    "plan": plan_name,
                    "suggestion_id": member["id"],
                    "precheck": check["snapshot"],
                    "approval_basis": {
                        "suggestion_id": member["id"],
                        "valid_until_ms": member["valid_until_ms"],
                        "independent": True,
                    },
                    "bundle_budget_before": {
                        "cost": bundle_cost,
                        "risk": bundle_risk,
                    },
                }
                reason = check["reason"]
                if reason == "already_effective":
                    member_record["outcome"] = "already_effective"
                elif blocked_by_mode:
                    member_record.update({
                        "outcome": "rejected",
                        "reason": reason or "bundle_precheck_failed",
                    })
                    rejected_ids.append(member["id"])
                    if reason == "scope_contention":
                        member_record["conflicting_intervention_ids"] = check[
                            "conflicts"]
                elif reason is not None:
                    member_record["outcome"] = (
                        "deferred" if reason == "scope_contention"
                        else "rejected")
                    member_record["reason"] = reason
                    if member_record["outcome"] == "rejected":
                        rejected_ids.append(member["id"])
                    if reason == "scope_contention":
                        member_record["conflicting_intervention_ids"] = check[
                            "conflicts"]
                    if reason in ("max_interventions_reached",
                                  "budget_exhausted",
                                  "risk_limit_reached"):
                        # 全局预算类失败触发停止，后续成员不再执行
                        # （失效不替换）；组合级预算不足不触发全局停止，
                        # 保留已执行集合，可经 resume_bundle 重开。
                        stopped = True
                        stop_reason = reason
                else:
                    inc = incremental_risk(member, executed_sources)
                    apply_member(member, check["candidate"], at_ms,
                                 risk_charge=inc)
                    executed_sources.update(member_risk_sources(member))
                    if member.get("mutex_group"):
                        executed_groups.add(member["mutex_group"])
                    bundle_cost += member["cost"]
                    bundle_risk += inc
                    # 已执行成员的成本与影响永久计入本组合，不因后续
                    # 失败、撤回或重排而归零。
                    committed_new.append({
                        "suggestion_id": member["id"],
                        "cost": member["cost"],
                        "risk": inc,
                        "effective_at_ms": at_ms,
                        "risk_sources": member_risk_sources(member),
                        "mutex_group": member.get("mutex_group"),
                        "permanent": True,
                    })
                    effective_ids.append(member["id"])
                    member_record.update({
                        "outcome": "confirmed",
                        "effective_at_ms": at_ms,
                        "incremental_risk": inc,
                        "budget_after": {
                            "cost": spent_cost,
                            "risk": spent_risk,
                            "interventions": intervention_count,
                        },
                        "bundle_budget_after": {
                            "cost": bundle_cost,
                            "risk": bundle_risk,
                        },
                    })
                member["events"].append(member_record)
                run["members"].append(member_record)
            run["status"] = ("rejected" if blocked_by_mode
                             else "partially_effective" if any_blocked
                             else "effective")
            if blocked_by_mode:
                run["reject_reason"] = "precheck_failed"
            committed_all = carried_committed + committed_new
            run["committed"] = committed_all
            run["committed_in_run"] = [c["suggestion_id"]
                                       for c in committed_new]
            run["effective_suggestion_ids"] = effective_ids
            run["rejected_suggestion_ids"] = rejected_ids
            shared_sources: dict = {}
            for committed in committed_new:
                shared_sources.update(committed["risk_sources"])
            run["risk_aggregation"] = {
                # 共享来源只计一次（并集），既不朴素相加也不重复折扣。
                "method": "shared_source_union",
                "shared_sources": shared_sources,
                "naive_sum": sum(by_id[c["suggestion_id"]]["risk"]
                                 for c in committed_new),
                "aggregated": sum(c["risk"] for c in committed_new),
            }
            run["remaining"] = [
                {"suggestion_id": m["suggestion_id"], "reason": m["reason"]}
                for m in run["members"]
                if m["outcome"] in ("rejected", "deferred")
                and by_id[m["suggestion_id"]]["status"] == "pending"
            ]
            run["resumable"] = bool(run["remaining"])
            run["budget_after"] = {
                "cost": spent_cost,
                "risk": spent_risk,
                "interventions": intervention_count,
            }
            run["bundle_budget_after"] = {
                "cost": bundle_cost,
                "risk": bundle_risk,
            }
            bundle_runs.append(run)
            return None

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
                            "risk_sources": dict(
                                template.get("risk_sources") or {}),
                            "mutex_group": template.get("mutex_group"),
                            "template": dict(template),
                            "status": "pending",
                            "events": [],
                        }
                        suggestions.append(suggestion)
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
                suggestion = find_suggestion(event)
                record = {
                    "at_ms": at_ms,
                    "action": action,
                    "suggestion_id": None if suggestion is None else suggestion["id"],
                }
                if action in ("confirm_bundle", "abandon_bundle",
                              "resume_bundle"):
                    # 组合事件写入 bundle 审计与各成员 events，不使用单条 record。
                    ignore_reason = handle_bundle_event(event, action, at_ms)
                    if ignore_reason is not None:
                        record.update({"outcome": "ignored",
                                       "reason": ignore_reason,
                                       "suggestion_id": None})
                    else:
                        record = None
                elif action == "manual_stop":
                    # 人工停止：保留已执行集合与未执行原因，可经
                    # resume_bundle 重开并逐项重新核对剩余部分。
                    stopped = True
                    stop_reason = "manual_stop"
                    record.update({"outcome": "stopped",
                                   "reason": "manual_stop"})
                elif suggestion is None:
                    record.update({"outcome": "ignored",
                                   "reason": "unknown_suggestion"})
                elif action == "confirm":
                    snapshot = precheck_snapshot(suggestion, at_ms)
                    check_reason, conflicts, candidate = validate_member(
                        suggestion, at_ms)
                    if check_reason == "scope_contention":
                        # 范围被在生效干预占用：保持待确认，可稍后再试。
                        record.update({
                            "outcome": "deferred",
                            "reason": "scope_contention",
                            "conflicting_intervention_ids": conflicts,
                            "precheck": snapshot,
                        })
                    elif check_reason is not None:
                        if check_reason in ("max_interventions_reached",
                                            "budget_exhausted",
                                            "risk_limit_reached"):
                            stopped = True
                            stop_reason = check_reason
                        record.update({"outcome": "rejected",
                                       "reason": check_reason,
                                       "precheck": snapshot})
                    else:
                        apply_member(suggestion, candidate, at_ms)
                        record.update({
                            "outcome": "confirmed",
                            "effective_at_ms": at_ms,
                            "precheck": snapshot,
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
                if record is not None and suggestion is not None:
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
                suggestions, bundle_runs)

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
