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
         bundle_runs, plan_runs) = self._run_treatment(
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
            "plans": plan_runs,
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
        plan_runs: list[dict] = []
        plans_by_id: dict[str, dict] = {}
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

        def apply_member(suggestion, candidate, at_ms):
            nonlocal spent_cost, spent_risk, intervention_count
            manager.start(candidate, at_ms)
            spent_cost += suggestion["cost"]
            spent_risk += suggestion["risk"]
            intervention_count += 1
            suggestion["status"] = "effective"
            suggestion["effective_at_ms"] = at_ms
            if candidate.end_ms is not None:
                push(candidate.end_ms, 1, "inject_end", {"id": candidate.id})
                scheduled_ends.append(candidate.end_ms)
            else:
                active_open_effect_ids.add(candidate.id)

        def handle_bundle_event(event, action, at_ms):
            """处理组合批准/放弃，返回组合不可解析时的忽略原因或 None。

            组合确认针对事件中显式列出的成员快照；失效成员不会被自动替换为
            其他动作。all 模式任一成员复核失败则整组不执行；partial 模式
            只执行通过复核的成员，已生效事实保留。
            """
            nonlocal stopped, stop_reason
            raw_ids = event.get("suggestion_ids")
            if not raw_ids:
                return "missing_suggestion_ids"
            by_id = {s["id"]: s for s in suggestions}
            unknown = [sid for sid in raw_ids if sid not in by_id]
            if unknown:
                return f"unknown_suggestion:{unknown[0]}"
            if len(set(raw_ids)) != len(raw_ids):
                return "duplicate_suggestion_ids"
            mode = event.get("mode", "all")
            run = {
                "id": f"bundle@{at_ms}:{len(bundle_runs)}",
                "at_ms": at_ms,
                "action": action,
                "mode": mode,
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
            # 执行前复核：对全部成员检查有效期、状态、范围与组合预算。
            checks = []
            started_specs = []
            projected_cost = spent_cost
            projected_risk = spent_risk
            projected_count = intervention_count
            max_count = limits.get("max_interventions")
            max_cost = limits.get("max_cost")
            max_risk = limits.get("max_risk")
            for member in ordered:
                snapshot = precheck_snapshot(member, at_ms)
                reason, conflicts, candidate = validate_member(
                    member, at_ms)
                budget_ok = True
                budget_reason = None
                if reason is None:
                    if (max_count is not None
                            and projected_count + 1 > int(max_count)):
                        budget_ok, budget_reason = False, "max_interventions_reached"
                    elif (max_cost is not None and projected_cost
                          + member["cost"] > float(max_cost)):
                        budget_ok, budget_reason = False, "budget_exhausted"
                    elif (max_risk is not None and projected_risk
                          + member["risk"] > float(max_risk)):
                        budget_ok, budget_reason = False, "risk_limit_reached"
                checks.append({
                    "member": member,
                    "snapshot": snapshot,
                    "reason": budget_reason if (reason is None and not budget_ok)
                    else reason,
                    "conflicts": conflicts,
                    "candidate": candidate,
                })
                if reason is None and budget_ok:
                    started_specs.append(build_candidate(member, at_ms))
                    projected_cost += member["cost"]
                    projected_risk += member["risk"]
                    projected_count += 1
            any_blocked = any(c["reason"] is not None for c in checks)
            effective_ids = []
            rejected_ids = []
            if mode == "all" and any_blocked:
                run["status"] = "rejected"
                run["reject_reason"] = "precheck_failed"
            else:
                run["status"] = "partially_effective" if any_blocked else "effective"
            for check in checks:
                member = check["member"]
                if not (mode == "all" and any_blocked):
                    # 逐项复核：快照与预算按该成员实际执行前的支出重新计算，
                    # 不能复用组合开始时的快照。
                    check["snapshot"] = precheck_snapshot(member, at_ms)
                    if check["reason"] is None:
                        if (max_count is not None
                                and intervention_count + 1 > int(max_count)):
                            check["reason"] = "max_interventions_reached"
                        elif (max_cost is not None and spent_cost
                              + member["cost"] > float(max_cost)):
                            check["reason"] = "budget_exhausted"
                        elif (max_risk is not None and spent_risk
                              + member["risk"] > float(max_risk)):
                            check["reason"] = "risk_limit_reached"
                member_record = {
                    "at_ms": at_ms,
                    "action": "confirm_bundle",
                    "bundle_id": run["id"],
                    "mode": mode,
                    "suggestion_id": member["id"],
                    "precheck": check["snapshot"],
                }
                blocked_by_mode = (mode == "all" and any_blocked)
                if blocked_by_mode:
                    member_record.update({
                        "outcome": "rejected",
                        "reason": check["reason"] or "bundle_precheck_failed",
                    })
                    rejected_ids.append(member["id"])
                    if check["reason"] == "scope_contention":
                        member_record["conflicting_intervention_ids"] = check[
                            "conflicts"]
                elif check["reason"] is not None:
                    member_record["outcome"] = (
                        "deferred" if check["reason"] == "scope_contention"
                        else "rejected")
                    member_record["reason"] = check["reason"]
                    if member_record["outcome"] == "rejected":
                        rejected_ids.append(member["id"])
                    if check["reason"] == "scope_contention":
                        member_record["conflicting_intervention_ids"] = check[
                            "conflicts"]
                    if check["reason"] in ("max_interventions_reached",
                                            "budget_exhausted",
                                            "risk_limit_reached"):
                        # 预算类失败触发停止，后续成员不再执行（失效不替换）。
                        stopped = True
                        stop_reason = check["reason"]
                else:
                    apply_member(member, check["candidate"], at_ms)
                    effective_ids.append(member["id"])
                    member_record.update({
                        "outcome": "confirmed",
                        "effective_at_ms": at_ms,
                        "budget_after": {
                            "cost": spent_cost,
                            "risk": spent_risk,
                            "interventions": intervention_count,
                        },
                    })
                member["events"].append(member_record)
                run["members"].append(member_record)
            run["effective_suggestion_ids"] = effective_ids
            run["rejected_suggestion_ids"] = rejected_ids
            run["budget_after"] = {
                "cost": spent_cost,
                "risk": spent_risk,
                "interventions": intervention_count,
            }
            bundle_runs.append(run)
            return None

        def aggregate_plan_risk(members):
            """共享风险来源聚合：同一来源内取成员最大风险、跨来源求和，
            未声明来源的成员各自独立计入。既不简单相加（把共享来源重复计费
            而高估），也不重复折扣（把独立来源一并折减而低估），避免伪可行方案。
            """
            by_source: dict[str, float] = {}
            independent = 0.0
            for member in members:
                sources = member.get("risk_sources") or []
                if not sources:
                    independent += member["risk"]
                for source in sources:
                    by_source[source] = max(by_source.get(source, 0.0),
                                            member["risk"])
            return independent + sum(by_source.values())

        def project_plan(order, budget):
            """按给定顺序模拟预算约束下的可执行集合（不改动任何状态）。"""
            projected_cost = 0.0
            executable: list[dict] = []
            executable_ids: list[str] = []
            for member in order:
                if member["status"] != "pending":
                    continue
                if budget is not None:
                    cap_cost = budget.get("cost")
                    cap_risk = budget.get("risk")
                    if (cap_cost is not None
                            and projected_cost + member["cost"]
                            > float(cap_cost)):
                        continue
                    if (cap_risk is not None
                            and aggregate_plan_risk(executable + [member])
                            > float(cap_risk)):
                        continue
                executable.append(member)
                executable_ids.append(member["id"])
                projected_cost += member["cost"]
            return {
                "order": [m["id"] for m in order],
                "projected_executable_ids": executable_ids,
                "projected_cost": projected_cost,
                "projected_risk": aggregate_plan_risk(executable),
            }

        def update_plan_status(run):
            if run["remaining_ids"]:
                run["status"] = ("stopped"
                                 if run["remaining_blocked_reason"] else "in_progress")
            elif run["executed_suggestion_ids"]:
                run["status"] = ("partially_effective"
                                 if run["failed_ids"] else "effective")
            else:
                run["status"] = "rejected"

        def plan_note_not_executed(run, suggestion_id, reason, at_ms):
            run["not_executed"].append({
                "suggestion_id": suggestion_id,
                "reason": reason,
                "at_ms": at_ms,
            })

        def plan_attempt(run, member, at_ms, substitutes, by_id, depth=0):
            """执行前按最新状态复核单项：有效期、证据、互斥范围、全局与
            组合预算。已生效成员视为重复完成通知，跳过且不重复计费。
            成员不可执行时若给出替代项，替代项必须是另一条独立待批准建议
            （独立批准依据），过期或已失效的许可不能沿用。
            返回追加到 run["members"] 的记录。
            """
            nonlocal stopped, stop_reason
            record = {
                "at_ms": at_ms,
                "action": run["current_action"],
                "plan_id": run["id"],
                "suggestion_id": member["id"],
                "precheck": precheck_snapshot(member, at_ms),
            }
            run["members"].append(record)

            def finish(outcome, reason=None, terminal=True, **extra):
                record["outcome"] = outcome
                if reason is not None:
                    record["reason"] = reason
                record.update(extra)
                if terminal and member["id"] in run["remaining_ids"]:
                    run["remaining_ids"].remove(member["id"])
                    plan_note_not_executed(run, member["id"],
                                           reason or outcome, at_ms)
                if terminal and outcome == "rejected":
                    run["failed_ids"].append(member["id"])
                update_plan_status(run)
                return record

            def reject_with_substitute(reason, blocks_plan=False, **extra):
                substitute_id = substitutes.get(member["id"])
                if substitute_id is not None and depth == 0:
                    substitute = by_id.get(substitute_id)
                    record["substitute_suggestion_id"] = substitute_id
                    if substitute is None:
                        record["substitute_reason"] = "unknown_substitute"
                    elif substitute["status"] != "pending":
                        record["substitute_reason"] = (
                            f"substitute_{substitute['status']}")
                    else:
                        finish("replaced", reason, **extra)
                        plan_attempt(run, substitute, at_ms, substitutes,
                                     by_id, depth + 1)
                        if substitute["id"] not in run["executed_suggestion_ids"]:
                            run["failed_ids"].append(member["id"])
                        update_plan_status(run)
                        return record
                if blocks_plan:
                    # 预算/证据类失败：保留已执行集合，剩余部分阻塞待重开复核。
                    run["remaining_blocked_reason"] = reason
                    run["stop_reason"] = reason
                    # 阻塞而非终结：成员留在剩余集合，重开后重新核对。
                    plan_note_not_executed(run, member["id"], reason, at_ms)
                    return finish("rejected", reason, terminal=False, **extra)
                return finish("rejected", reason, **extra)

            if run["remaining_blocked_reason"] is not None:
                reason = f"plan_stopped:{run['remaining_blocked_reason']}"
                plan_note_not_executed(run, member["id"], reason, at_ms)
                return finish("skipped", reason, terminal=False)
            # 重复完成通知：已生效成员不重复执行、不重复计入预算。
            if member["status"] == "effective":
                return finish("skipped", "already_effective")
            if member["status"] != "pending":
                return reject_with_substitute(f"suggestion_{member['status']}")
            if not member["basis"]["window_observation_ids"]:
                return reject_with_substitute("missing_evidence",
                                              blocks_plan=True)
            budget = run.get("budget")
            if budget is not None:
                cap_cost = budget.get("cost")
                cap_risk = budget.get("risk")
                if (cap_cost is not None
                        and run["spent"]["cost"] + member["cost"]
                        > float(cap_cost)):
                    return reject_with_substitute("plan_budget_exceeded",
                                                  blocks_plan=True)
                if (cap_risk is not None
                        and aggregate_plan_risk(
                            run["executed_members"] + [member])
                        > float(cap_risk)):
                    return reject_with_substitute("plan_budget_exceeded",
                                                  blocks_plan=True)
            check_reason, conflicts, candidate = validate_member(member, at_ms)
            # 同一组合成员（含替代项）视为共同批准，互不占范围；
            # 只与组合外的在生效干预争用。
            co_approved = set(run["suggestion_ids"]) | set(substitutes.values())
            conflicts = [c for c in conflicts if c not in co_approved]
            if check_reason == "scope_contention" and not conflicts:
                check_reason = None
            if check_reason == "scope_contention":
                # 互斥范围被占用：暂缓，成员保留在剩余集合中。
                return finish("deferred", "scope_contention", terminal=False,
                              conflicting_intervention_ids=conflicts)
            if check_reason is not None:
                if check_reason in ("max_interventions_reached",
                                    "budget_exhausted",
                                    "risk_limit_reached"):
                    stopped = True
                    stop_reason = check_reason
                return reject_with_substitute(check_reason)
            apply_member(member, candidate, at_ms)
            run["executed_members"].append(member)
            run["executed_suggestion_ids"].append(member["id"])
            if member["id"] in run["remaining_ids"]:
                run["remaining_ids"].remove(member["id"])
            # 已执行干预的成本与影响永久计入本次组合，只增不减。
            run["spent"]["cost"] += member["cost"]
            run["spent"]["risk"] = aggregate_plan_risk(run["executed_members"])
            run["budget_trajectory"].append({
                "at_ms": at_ms,
                "suggestion_id": member["id"],
                "spent_cost": run["spent"]["cost"],
                "spent_risk": run["spent"]["risk"],
            })
            record.update({
                "outcome": "confirmed",
                "effective_at_ms": at_ms,
                "plan_spent_after": dict(run["spent"]),
            })
            update_plan_status(run)
            return record

        def handle_plan_event(event, action, at_ms):
            """剩余方案选择：批准时给定组合预算，执行每一项前按最新状态复核
            剩余候选；显式比较“保持原顺序”与“低风险优先”两种方案。
            """
            nonlocal stopped, stop_reason
            by_id = {s["id"]: s for s in suggestions}
            if action == "plan_bundle":
                raw_ids = event.get("suggestion_ids")
                if not raw_ids:
                    return "missing_suggestion_ids"
                unknown = [sid for sid in raw_ids if sid not in by_id]
                if unknown:
                    return f"unknown_suggestion:{unknown[0]}"
                if len(set(raw_ids)) != len(raw_ids):
                    return "duplicate_suggestion_ids"
                strategy = event.get("strategy", "original_order")
                if strategy not in ("original_order", "lower_risk_first"):
                    return "unknown_strategy"
                plan_id = event.get("plan_id") or f"plan@{at_ms}:{len(plan_runs)}"
                if plan_id in plans_by_id:
                    return "duplicate_plan_id"
                ordered = [by_id[sid] for sid in raw_ids]
                lower_risk = sorted(
                    ordered, key=lambda s: (s["risk"], s["cost"], s["id"]))
                budget = event.get("budget")
                budget = None if budget is None else {
                    "cost": (None if budget.get("cost") is None
                             else float(budget["cost"])),
                    "risk": (None if budget.get("risk") is None
                             else float(budget["risk"])),
                }
                plans = {
                    "original_order": list(raw_ids),
                    "lower_risk_first": [m["id"] for m in lower_risk],
                }
                run = {
                    "id": plan_id,
                    "at_ms": at_ms,
                    "action": "plan_bundle",
                    "strategy": strategy,
                    "budget": budget,
                    "risk_aggregation":
                        "max_within_shared_source_sum_across_sources",
                    "suggestion_ids": list(raw_ids),
                    "plans": plans,
                    "plan_comparison": {
                        "original_order": project_plan(ordered, budget),
                        "lower_risk_first": project_plan(lower_risk, budget),
                    },
                    "members": [],
                    "events": [{"at_ms": at_ms, "action": "plan_bundle",
                                "strategy": strategy, "budget": budget}],
                    "executed_members": [],
                    "executed_suggestion_ids": [],
                    "failed_ids": [],
                    "not_executed": [],
                    "remaining_ids": list(raw_ids),
                    "remaining_blocked_reason": None,
                    "spent": {"cost": 0.0, "risk": 0.0},
                    "spent_permanent": True,
                    "budget_trajectory": [],
                    "status": "in_progress",
                    "stop_reason": None,
                    "current_action": "plan_bundle",
                }
                plan_runs.append(run)
                plans_by_id[plan_id] = run
                substitutes = event.get("substitutes") or {}
                execution_order = ordered if strategy == "original_order" else lower_risk
                for member in execution_order:
                    plan_attempt(run, member, at_ms, substitutes, by_id)
                update_plan_status(run)
                return None
            plan_id = event.get("plan_id")
            run = plans_by_id.get(plan_id) if plan_id else None
            if run is None:
                return "unknown_plan"
            if action == "stop_bundle":
                # 人工停止：保留已执行集合与每项未执行原因。
                run["events"].append({"at_ms": at_ms, "action": "stop_bundle"})
                run["remaining_blocked_reason"] = "manual_stop"
                run["stop_reason"] = "manual_stop"
                for sid in list(run["remaining_ids"]):
                    member = by_id[sid]
                    run["members"].append({
                        "at_ms": at_ms,
                        "action": "stop_bundle",
                        "plan_id": run["id"],
                        "suggestion_id": sid,
                        "precheck": precheck_snapshot(member, at_ms),
                        "outcome": "skipped",
                        "reason": "manual_stop",
                    })
                    plan_note_not_executed(run, sid, "manual_stop", at_ms)
                update_plan_status(run)
                return None
            if action == "revise_budget":
                # 预算突增/削减：新预算立即生效；已执行支出永久保留，
                # 剩余计划若超出新预算则阻塞并记录原因。
                new_budget = event.get("budget")
                if new_budget is None:
                    return "missing_budget"
                new_budget = {
                    "cost": (None if new_budget.get("cost") is None
                             else float(new_budget["cost"])),
                    "risk": (None if new_budget.get("risk") is None
                             else float(new_budget["risk"])),
                }
                old_budget = run["budget"]
                run["budget"] = new_budget
                run["events"].append({
                    "at_ms": at_ms,
                    "action": "revise_budget",
                    "old_budget": old_budget,
                    "new_budget": new_budget,
                    "spent_at_revision": dict(run["spent"]),
                })
                remaining = [by_id[sid] for sid in run["remaining_ids"]]
                projection = project_plan(
                    [m for m in remaining if m["status"] == "pending"],
                    new_budget)
                fits = True
                if new_budget is not None:
                    cap_cost = new_budget.get("cost")
                    cap_risk = new_budget.get("risk")
                    if (cap_cost is not None
                            and run["spent"]["cost"]
                            + projection["projected_cost"] > float(cap_cost)):
                        fits = False
                    if (cap_risk is not None
                            and aggregate_plan_risk(
                                run["executed_members"]
                                + [by_id[sid] for sid in
                                   projection["projected_executable_ids"]])
                            > float(cap_risk)):
                        fits = False
                if not fits:
                    run["remaining_blocked_reason"] = "budget_reduced"
                    run["stop_reason"] = "budget_reduced"
                    for sid in list(run["remaining_ids"]):
                        plan_note_not_executed(run, sid, "budget_reduced", at_ms)
                update_plan_status(run)
                return None
            if action == "resume_bundle":
                # 重开：只继续经重新核对的剩余部分；已执行事实不变。
                run["events"].append({"at_ms": at_ms, "action": "resume_bundle"})
                run["remaining_blocked_reason"] = None
                run["current_action"] = "resume_bundle"
                for sid in list(run["remaining_ids"]):
                    plan_attempt(run, by_id[sid], at_ms,
                                 event.get("substitutes") or {}, by_id)
                run["current_action"] = "plan_bundle"
                update_plan_status(run)
                return None
            return "unknown_action"

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
                            "risk_sources": list(template.get("risk_sources", [])),
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
                if action in ("confirm_bundle", "abandon_bundle"):
                    # 组合事件写入 bundle 审计与各成员 events，不使用单条 record。
                    ignore_reason = handle_bundle_event(event, action, at_ms)
                    if ignore_reason is not None:
                        record.update({"outcome": "ignored",
                                       "reason": ignore_reason,
                                       "suggestion_id": None})
                    else:
                        record = None
                elif action in ("plan_bundle", "stop_bundle", "revise_budget",
                                "resume_bundle"):
                    # 剩余方案选择事件写入 plan 审计，不使用单条 record。
                    ignore_reason = handle_plan_event(event, action, at_ms)
                    if ignore_reason is not None:
                        record.update({"outcome": "ignored",
                                       "reason": ignore_reason,
                                       "suggestion_id": None})
                    else:
                        record = None
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
                suggestions, bundle_runs, plan_runs)

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
