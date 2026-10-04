"""组合批准（bundle）生命周期测试。"""
from pathlib import Path

from chaos_platform import (
    ExperimentRunner,
    RequestSpec,
    Scenario,
    save_adaptive_run,
    load_run,
)
from chaos_platform.service_chain import CallResult
from chaos_platform.scenario import scenario_from_dict
from tests.test_platform import _FailingChain, _APPROVAL_THRESHOLDS


def _bundle_scenario(limits=None, expires_after_ms=200):
    policy = {
        "decision_at_ms": [100, 110],
        "limits": limits or {},
        "approval": {"required": True,
                     "expires_after_ms": expires_after_ms,
                     "allow_overlap": False},
        "actions": {"failed": {
            "type": "interruption", "target": "inventory",
            "duration_ms": 100, "params": {}, "cost": 1, "risk": 1}},
    }
    return Scenario(
        name="bundle", seed=7,
        requests=[RequestSpec(50, {"i": 0}), RequestSpec(90, {"i": 1})],
        adaptive_policy=policy)


def _run_bundle(events, **kwargs):
    return ExperimentRunner(_FailingChain()).run_adaptive_branch(
        _bundle_scenario(**kwargs), dict(_APPROVAL_THRESHOLDS),
        approvals=events)


B_IDS = ["root:d0:failed", "root:d1:failed"]


def test_bundle_all_mode_late_confirm_is_all_or_nothing():
    # 迟到确认：两条建议都已过期，all 模式整组不执行。
    branch = _run_bundle([
        {"at_ms": 400, "action": "confirm_bundle",
         "suggestion_ids": B_IDS, "mode": "all"}], expires_after_ms=100)
    run = branch["bundles"][0]
    assert run["status"] == "rejected"
    assert run["reject_reason"] == "precheck_failed"
    assert run["effective_suggestion_ids"] == []
    reasons = {m["suggestion_id"]: m["reason"] for m in run["members"]}
    assert reasons["root:d0:failed"] == "suggestion_expired"
    assert reasons["root:d1:failed"] == "suggestion_expired"
    # 失效成员不会被自动替换为另一动作。
    assert {s["status"] for s in branch["suggestions"]} == {"expired"}
    assert branch["fault_events"] == []
    assert all(not m["precheck"]["within_validity"] for m in run["members"])


def test_bundle_condition_changes_all_mode_blocks_whole_group():
    # 条件变化：第一项仍有效，第二项被其它在生效干预占用（scope_contention）。
    # 针对旧条件的整组确认不能应用到剩余的第一项：all 模式整组不执行。
    branch = _run_bundle([
        {"decision_index": 0, "at_ms": 105, "action": "confirm"},
        {"at_ms": 115, "action": "confirm_bundle",
         "suggestion_ids": B_IDS, "mode": "all"}])
    first, second = branch["suggestions"]
    assert first["status"] == "effective"
    run = branch["bundles"][0]
    assert run["status"] == "rejected"
    assert run["reject_reason"] == "precheck_failed"
    assert run["effective_suggestion_ids"] == []
    member_second = next(m for m in run["members"]
                         if m["suggestion_id"] == second["id"])
    assert member_second["reason"] == "scope_contention"
    assert member_second["conflicting_intervention_ids"] == [first["id"]]
    # 第二项保持 pending，未被偷偷执行或替换。
    assert second["status"] == "pending"


def test_bundle_all_mode_succeeds_when_all_still_valid():
    branch = _run_bundle([
        {"at_ms": 120, "action": "confirm_bundle",
         "suggestion_ids": B_IDS, "mode": "all"}])
    run = branch["bundles"][0]
    assert run["status"] == "effective"
    assert run["effective_suggestion_ids"] == B_IDS
    starts = {e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"}
    assert starts == set(B_IDS)


def test_bundle_partial_mode_skips_expired_but_keeps_remaining():
    # partial 模式：旧条件下失效的成员跳过，仅执行仍有效成员，不替换。
    branch = _run_bundle([
        {"at_ms": 205, "action": "confirm_bundle",
         "suggestion_ids": B_IDS, "mode": "partial"}],
        expires_after_ms=100)
    run = branch["bundles"][0]
    assert run["status"] == "partially_effective"
    assert run["effective_suggestion_ids"] == ["root:d1:failed"]
    rejected = {m["suggestion_id"]: m for m in run["members"]
                if m["outcome"] == "rejected"}
    assert rejected["root:d0:failed"]["reason"] == "suggestion_expired"
    statuses = {s["id"]: s["status"] for s in branch["suggestions"]}
    assert statuses["root:d0:failed"] == "expired"
    assert statuses["root:d1:failed"] == "effective"


def test_bundle_partial_mode_preserves_executed_facts_on_stop():
    # 组合预算只允许一个干预：第一项生效后第二项触发停止，事实保留。
    branch = _run_bundle([
        {"at_ms": 120, "action": "confirm_bundle",
         "suggestion_ids": B_IDS, "mode": "partial"}],
        limits={"max_interventions": 1})
    run = branch["bundles"][0]
    assert run["status"] == "partially_effective"
    assert run["effective_suggestion_ids"] == ["root:d0:failed"]
    second = next(m for m in run["members"]
                  if m["suggestion_id"] == "root:d1:failed")
    assert second["outcome"] == "rejected"
    assert second["reason"] == "max_interventions_reached"
    assert branch["stopped"] is True
    assert branch["stop_reason"] == "max_interventions_reached"
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == ["root:d0:failed"]
    statuses = {s["id"]: s["status"] for s in branch["suggestions"]}
    assert statuses["root:d0:failed"] == "effective"


def test_bundle_abandon_vs_repropose_and_save_restore(tmp_path: Path):
    # 比较“放弃组合”与“重新确认组合”两个分支，观察相同、结果不同，
    # 保存恢复后 bundles 生命周期一致。
    scenario = _bundle_scenario()
    thresholds = dict(_APPROVAL_THRESHOLDS)
    runner = ExperimentRunner(_FailingChain())
    abandoned = runner.run_adaptive_branch(scenario, thresholds, approvals=[
        {"at_ms": 120, "action": "abandon_bundle",
         "suggestion_ids": B_IDS}])
    rerun = runner.run_adaptive_branch(scenario, thresholds, approvals=[
        {"at_ms": 115, "action": "confirm_bundle",
         "suggestion_ids": B_IDS, "mode": "all"}])
    assert abandoned["bundles"][0]["status"] == "abandoned"
    assert {s["status"] for s in abandoned["suggestions"]} == {"abandoned"}
    assert abandoned["fault_events"] == []
    assert rerun["bundles"][0]["status"] == "effective"
    assert abandoned["observations"] == rerun["observations"]
    assert (abandoned["control_observations"]
            == rerun["control_observations"])
    assert len(rerun["fault_events"]) > 0
    experiment = {"scenario_name": scenario.name, "seed": scenario.seed,
                  "branches": [abandoned, rerun]}
    path = tmp_path / "bundles.json"
    save_adaptive_run(experiment, scenario, path)
    saved = load_run(path)
    restored_abandoned, restored_rerun = saved["result"]["branches"]
    assert restored_abandoned["bundles"] == abandoned["bundles"]
    assert restored_rerun["bundles"] == rerun["bundles"]
    replayed = runner.run_adaptive_branch(
        scenario_from_dict(saved["scenario"]), thresholds,
        approvals=saved["result"]["branches"][1]["approvals"])
    assert replayed["bundles"] == rerun["bundles"]


def test_bundle_basis_and_precheck_remain_auditable():
    branch = _run_bundle([
        {"at_ms": 120, "action": "confirm_bundle",
         "suggestion_ids": B_IDS, "mode": "partial"}])
    first, second = branch["suggestions"]
    assert first["basis"]["window_observation_ids"] == [0]
    assert second["basis"]["window_observation_ids"] == [1]
    member = next(m for m in branch["bundles"][0]["members"]
                  if m["suggestion_id"] == first["id"])
    assert member["precheck"]["checked_at_ms"] == 120
    assert member["precheck"]["within_validity"] is True
    assert member["precheck"]["budget_before"] == {
        "cost": 0.0, "risk": 0.0, "interventions": 0}
