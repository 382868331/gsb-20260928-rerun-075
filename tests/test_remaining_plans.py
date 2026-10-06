"""组合干预在执行中变化条件下的剩余方案选择测试。"""
from pathlib import Path

from chaos_platform import (
    ExperimentRunner,
    RequestSpec,
    Scenario,
    load_run,
    save_adaptive_run,
)
from chaos_platform.scenario import scenario_from_dict
from chaos_platform.service_chain import CallResult
from tests.test_platform import _FailingChain, _APPROVAL_THRESHOLDS


def _scenario(policy_actions, limits=None, expires_after_ms=200,
              decision_at_ms=(100, 110), requests=None):
    policy = {
        "decision_at_ms": list(decision_at_ms),
        "limits": limits or {},
        "approval": {"required": True,
                     "expires_after_ms": expires_after_ms,
                     "allow_overlap": False},
        "actions": policy_actions,
    }
    return Scenario(
        name="remaining", seed=7,
        requests=requests or [RequestSpec(50, {"i": 0}),
                              RequestSpec(90, {"i": 1})],
        adaptive_policy=policy)


def _failed_action(**extra):
    action = {"type": "interruption", "target": "inventory",
              "duration_ms": 100, "params": {}, "cost": 1, "risk": 1}
    action.update(extra)
    return action


IDS = ["root:d0:failed", "root:d1:failed"]


def test_shared_risk_sources_aggregate_by_union_not_naive_sum():
    # 两项干预共享同一风险来源：并集只计一次，朴素相加会伪不可行，
    # 重复折扣会伪可行；聚合方式须显式记录。
    scenario = _scenario(
        {"failed": _failed_action(risk=2, risk_sources={"shared_net": 2})},
        limits={"max_risk": 3})
    branch = ExperimentRunner(_FailingChain()).run_adaptive_branch(
        scenario, dict(_APPROVAL_THRESHOLDS),
        approvals=[{"at_ms": 120, "action": "confirm_bundle",
                    "suggestion_ids": IDS, "mode": "partial"}])
    run = branch["bundles"][0]
    assert run["status"] == "effective"
    assert run["effective_suggestion_ids"] == IDS
    aggregation = run["risk_aggregation"]
    assert aggregation["method"] == "shared_source_union"
    assert aggregation["shared_sources"] == {"shared_net": 2}
    assert aggregation["naive_sum"] == 4
    assert aggregation["aggregated"] == 2
    # 预算轨迹按聚合后风险记账，而非朴素相加。
    assert run["budget_after"]["risk"] == 2.0
    assert run["committed"][1]["risk"] == 0


def test_mutex_group_excludes_remaining_candidate():
    scenario = _scenario({"failed": _failed_action(mutex_group="g")})
    branch = ExperimentRunner(_FailingChain()).run_adaptive_branch(
        scenario, dict(_APPROVAL_THRESHOLDS),
        approvals=[{"at_ms": 120, "action": "confirm_bundle",
                    "suggestion_ids": IDS, "mode": "partial"}])
    run = branch["bundles"][0]
    assert run["status"] == "partially_effective"
    assert run["effective_suggestion_ids"] == [IDS[0]]
    second = run["members"][1]
    assert second["outcome"] == "rejected"
    assert second["reason"] == "mutex_conflict"
    assert run["remaining"] == [{"suggestion_id": IDS[1],
                                 "reason": "mutex_conflict"}]


def test_resume_rechecks_remaining_and_expired_permit_is_not_reused():
    # 组合预算突增：只执行第一项，保留未执行原因；重开时逐项重新核对，
    # 仍在有效期内的剩余项可继续，过期许可不能沿用。
    scenario = _scenario({"failed": _failed_action()}, expires_after_ms=200)
    runner = ExperimentRunner(_FailingChain())
    thresholds = dict(_APPROVAL_THRESHOLDS)
    first = [{"at_ms": 120, "action": "confirm_bundle",
              "suggestion_ids": IDS, "mode": "partial",
              "budget": {"max_cost": 1}}]
    # 重开时仍在有效期内（d1 有效至 310，d0 效果 220 结束）：
    # 剩余项经重新核对后继续。
    branch = runner.run_adaptive_branch(
        scenario, thresholds, approvals=first + [
            {"at_ms": 250, "action": "resume_bundle",
             "bundle_id": "bundle@120:0", "budget": {"max_cost": 2}}])
    run1, run2 = branch["bundles"]
    assert run1["status"] == "partially_effective"
    assert run1["effective_suggestion_ids"] == [IDS[0]]
    assert run1["remaining"] == [{"suggestion_id": IDS[1],
                                  "reason": "bundle_budget_exceeded"}]
    assert run1["resumable"] is True
    assert run2["resumed_from"] == run1["id"]
    assert run2["effective_suggestion_ids"] == [IDS[1]]
    # 已执行事实永久结转：重开后的 committed 含第一项，成本不归零。
    assert [c["suggestion_id"] for c in run2["committed"]] == IDS
    assert run2["committed"][0]["permanent"] is True
    assert run2["bundle_budget_after"]["cost"] == 2.0
    # 重开时许可已过期（d1 有效至 310，350 重开）：不能沿用过期许可。
    branch = runner.run_adaptive_branch(
        scenario, thresholds, approvals=first + [
            {"at_ms": 350, "action": "resume_bundle",
             "bundle_id": "bundle@120:0", "budget": {"max_cost": 2}}])
    run2 = branch["bundles"][1]
    assert run2["effective_suggestion_ids"] == []
    member = run2["members"][0]
    assert member["outcome"] == "rejected"
    assert member["reason"] == "suggestion_expired"
    assert member["precheck"]["within_validity"] is False
    statuses = {s["id"]: s["status"] for s in branch["suggestions"]}
    assert statuses[IDS[0]] == "effective"
    assert statuses[IDS[1]] == "expired"
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == [IDS[0]]


def test_manual_stop_preserves_executed_and_resume_rechecks():
    scenario = _scenario({"failed": _failed_action()})
    branch = ExperimentRunner(_FailingChain()).run_adaptive_branch(
        scenario, dict(_APPROVAL_THRESHOLDS), approvals=[
            {"at_ms": 120, "action": "confirm_bundle",
             "suggestion_ids": IDS, "mode": "partial",
             "budget": {"max_cost": 1}},
            {"at_ms": 130, "action": "manual_stop"},
            {"decision_index": 1, "at_ms": 140, "action": "confirm"},
            {"at_ms": 250, "action": "resume_bundle",
             "bundle_id": "bundle@120:0", "budget": {"max_cost": 2}}])
    # 人工停止期间，单条确认不能绕过停止条件。
    confirm = next(e for e in branch["suggestions"][1]["events"]
                   if e["action"] == "confirm")
    assert confirm["outcome"] == "rejected"
    assert confirm["reason"] == "stop_condition_active"
    run2 = branch["bundles"][1]
    assert run2["effective_suggestion_ids"] == [IDS[1]]
    assert branch["stopped"] is False
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == IDS


def test_duplicate_completion_notification_does_not_double_count():
    scenario = _scenario({"failed": _failed_action()})
    branch = ExperimentRunner(_FailingChain()).run_adaptive_branch(
        scenario, dict(_APPROVAL_THRESHOLDS), approvals=[
            {"at_ms": 120, "action": "confirm_bundle",
             "suggestion_ids": IDS, "mode": "all"},
            {"at_ms": 130, "action": "confirm_bundle",
             "suggestion_ids": IDS, "mode": "all"}])
    run1, run2 = branch["bundles"]
    assert run1["status"] == "effective"
    assert [m["outcome"] for m in run2["members"]] == [
        "already_effective", "already_effective"]
    # 预算轨迹不因重复通知重复扣减。
    assert run2["budget_after"] == run1["budget_after"]
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == IDS


def test_withdraw_does_not_zero_committed_cost_and_impact():
    scenario = _scenario(
        {"failed": _failed_action()},
        requests=[RequestSpec(50, {"i": 0}), RequestSpec(90, {"i": 1}),
                  RequestSpec(150, {"i": 2})])
    branch = ExperimentRunner(_FailingChain()).run_adaptive_branch(
        scenario, dict(_APPROVAL_THRESHOLDS), approvals=[
            {"at_ms": 120, "action": "confirm_bundle",
             "suggestion_ids": IDS, "mode": "all"},
            {"decision_index": 0, "at_ms": 160, "action": "withdraw"}])
    run = branch["bundles"][0]
    first = branch["suggestions"][0]
    assert first["status"] == "withdrawn"
    # 已执行成本与影响永久计入：撤回不归零。
    assert run["budget_after"]["cost"] == 2.0
    committed = {c["suggestion_id"]: c for c in run["committed"]}
    assert committed[IDS[0]]["permanent"] is True
    assert committed[IDS[0]]["cost"] == 1
    assert first["affected_observation_ids"] == [2]


class _MixedLatencyChain:
    """请求 0 低延迟、请求 1 高延迟，让两次决策落到不同严重度。"""

    def call(self, request_id, payload, seed, manager):
        latency_ms = 20 if request_id == 0 else 200
        return CallResult(request_id, payload, "interrupted",
                          latency_ms * 1_000_000,
                          failed_node="inventory", error="stub")


def _two_severity_scenario():
    return _scenario(
        {"failed": _failed_action(risk=3),
         "degraded": _failed_action(risk=1)},
        expires_after_ms=500,
        decision_at_ms=(100, 300))


MIXED_THRESHOLDS = {"window_ms": 30, "min_observations": 1,
                    "failure_rate": 1.0, "avg_latency_ms": 100}
MIXED_IDS = ["root:d0:failed", "root:d1:degraded"]


def _run_plan(plan):
    return ExperimentRunner(_MixedLatencyChain()).run_adaptive_branch(
        _two_severity_scenario(), dict(MIXED_THRESHOLDS),
        approvals=[{"at_ms": 310, "action": "confirm_bundle",
                    "suggestion_ids": MIXED_IDS, "mode": "partial",
                    "plan": plan, "budget": {"max_risk": 3}}])


def test_remaining_plan_compares_original_order_and_lowest_risk():
    # 同一批剩余候选、同一组合风险预算：两种明确方案得出不同结论，
    # 两次推演都保存，实际执行选定方案。
    original = _run_plan("original_order")
    lowest = _run_plan("lowest_risk")
    run = original["bundles"][0]
    assert run["plan_comparison"]["selected"] == "original_order"
    plans = {p["name"]: p for p in run["plan_comparison"]["plans"]}
    assert plans["original_order"]["order"] == MIXED_IDS
    assert plans["lowest_risk"]["order"] == MIXED_IDS[::-1]
    assert [e["suggestion_id"] for e in plans["original_order"]["entries"]
            if e["outcome"] == "executable"] == [MIXED_IDS[0]]
    assert [e["suggestion_id"] for e in plans["lowest_risk"]["entries"]
            if e["outcome"] == "executable"] == [MIXED_IDS[1]]
    # 保持原顺序：执行高风险项后低风险项超出组合预算。
    assert run["effective_suggestion_ids"] == [MIXED_IDS[0]]
    assert run["remaining"] == [{"suggestion_id": MIXED_IDS[1],
                                 "reason": "bundle_budget_exceeded"}]
    # 选择低风险替代项：低风险项先执行，高风险项被排除。
    run_low = lowest["bundles"][0]
    assert run_low["effective_suggestion_ids"] == [MIXED_IDS[1]]
    assert run_low["remaining"] == [{"suggestion_id": MIXED_IDS[0],
                                     "reason": "bundle_budget_exceeded"}]
    # 替代项使用自己的独立批准依据，不沿用被替换项的许可。
    member = run_low["members"][0]
    assert member["suggestion_id"] == MIXED_IDS[1]
    assert member["approval_basis"] == {
        "suggestion_id": MIXED_IDS[1],
        "valid_until_ms": 800,
        "independent": True,
    }


def test_remaining_plans_survive_save_and_replay(tmp_path: Path):
    scenario = _two_severity_scenario()
    thresholds = dict(MIXED_THRESHOLDS)
    runner = ExperimentRunner(_MixedLatencyChain())
    branch = runner.run_adaptive_branch(scenario, thresholds, approvals=[
        {"at_ms": 310, "action": "confirm_bundle",
         "suggestion_ids": MIXED_IDS, "mode": "partial",
         "plan": "lowest_risk", "budget": {"max_risk": 3}},
        {"at_ms": 420, "action": "resume_bundle",
         "bundle_id": "bundle@310:0", "budget": {"max_risk": 4}}])
    run1, run2 = branch["bundles"]
    assert run2["resumed_from"] == run1["id"]
    assert run2["effective_suggestion_ids"] == [MIXED_IDS[0]]
    assert [c["suggestion_id"] for c in run2["committed"]] == [
        MIXED_IDS[1], MIXED_IDS[0]]
    experiment = {"scenario_name": scenario.name, "seed": scenario.seed,
                  "branches": [branch]}
    path = tmp_path / "remaining.json"
    save_adaptive_run(experiment, scenario, path)
    saved = load_run(path)
    restored = saved["result"]["branches"][0]
    assert restored["bundles"] == branch["bundles"]
    replayed = runner.run_adaptive_branch(
        scenario_from_dict(saved["scenario"]), thresholds,
        approvals=restored["approvals"])
    assert replayed["bundles"] == branch["bundles"]
