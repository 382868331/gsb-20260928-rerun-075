"""剩余方案选择（plan）生命周期测试：共享风险聚合、部分执行后许可过期、
重复完成通知、人工停止/预算修订/重开。"""
from pathlib import Path

from chaos_platform import (
    ExperimentRunner,
    RequestSpec,
    Scenario,
    save_adaptive_run,
    load_run,
)
from chaos_platform.scenario import scenario_from_dict
from tests.test_platform import _FailingChain, _APPROVAL_THRESHOLDS


def _plan_scenario(limits=None, expires_after_ms=200, risk_sources=None):
    action = {
        "type": "interruption", "target": "inventory",
        "duration_ms": 100, "params": {}, "cost": 1, "risk": 1,
    }
    if risk_sources is not None:
        action["risk_sources"] = list(risk_sources)
    policy = {
        "decision_at_ms": [100, 115, 140],
        "limits": limits or {},
        "approval": {"required": True,
                     "expires_after_ms": expires_after_ms,
                     "allow_overlap": False},
        "actions": {"failed": action},
    }
    return Scenario(
        name="plan", seed=7,
        requests=[RequestSpec(50, {"i": 0}), RequestSpec(85, {"i": 1}),
                  RequestSpec(105, {"i": 2})],
        adaptive_policy=policy)


P_IDS = ["root:d0:failed", "root:d1:failed", "root:d2:failed"]


def _run_plan(events, **kwargs):
    return ExperimentRunner(_FailingChain()).run_adaptive_branch(
        _plan_scenario(**kwargs), dict(_APPROVAL_THRESHOLDS),
        approvals=events)


def test_shared_risk_source_aggregates_by_max_not_sum():
    # 三项共享同一风险来源：聚合风险取来源内最大值（1）而非简单相加（3），
    # 风险预算 1 下三项都可执行；成本仍逐项永久计入。
    branch = _run_plan([
        {"at_ms": 160, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS, "budget": {"cost": 3, "risk": 1}}],
        risk_sources=["shared"])
    run = branch["plans"][0]
    assert run["risk_aggregation"] == "max_within_shared_source_sum_across_sources"
    assert run["status"] == "effective"
    assert run["executed_suggestion_ids"] == P_IDS
    assert run["spent"] == {"cost": 3.0, "risk": 1.0}
    assert [t["spent_risk"] for t in run["budget_trajectory"]] == [1.0, 1.0, 1.0]
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == P_IDS


def test_independent_risk_not_double_discounted_into_pseudo_feasible():
    # 无共享来源时三项风险独立计入（聚合 3）：风险预算 1.5 只允许第一项，
    # 其余被阻塞；若重复折扣（全部取 max=1）会得出伪可行方案。
    branch = _run_plan([
        {"at_ms": 160, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS, "budget": {"cost": 3, "risk": 1.5}}])
    run = branch["plans"][0]
    assert run["executed_suggestion_ids"] == P_IDS[:1]
    members = {m["suggestion_id"]: m for m in run["members"]}
    assert members[P_IDS[1]]["outcome"] == "rejected"
    assert members[P_IDS[1]]["reason"] == "plan_budget_exceeded"
    assert members[P_IDS[2]]["outcome"] == "skipped"
    assert members[P_IDS[2]]["reason"] == "plan_stopped:plan_budget_exceeded"
    assert run["status"] == "stopped"
    assert run["stop_reason"] == "plan_budget_exceeded"
    # 已执行事实与支出保留，未执行原因记录。
    assert run["spent"] == {"cost": 1.0, "risk": 1.0}
    assert run["spent_permanent"] is True
    assert {n["suggestion_id"] for n in run["not_executed"]} == set(P_IDS[1:])
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == P_IDS[:1]


def test_plan_comparison_and_strategy_selection():
    # 两种明确方案：保持原顺序 vs 低风险优先；预算只允许一项时，
    # 不同策略执行不同成员，比较快照随报告保存。
    branch = _run_plan([
        {"at_ms": 160, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": [P_IDS[2], P_IDS[0]],
         "strategy": "lower_risk_first",
         "budget": {"cost": 1}}])
    run = branch["plans"][0]
    assert run["plans"]["original_order"] == [P_IDS[2], P_IDS[0]]
    assert run["plans"]["lower_risk_first"] == [P_IDS[0], P_IDS[2]]
    assert run["plan_comparison"]["original_order"]["order"] == [P_IDS[2], P_IDS[0]]
    # 低风险优先（同风险按成本、ID 排序）先执行 d0。
    assert run["executed_suggestion_ids"] == [P_IDS[0]]
    original = _run_plan([
        {"at_ms": 160, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": [P_IDS[2], P_IDS[0]],
         "strategy": "original_order",
         "budget": {"cost": 1}}])
    assert original["plans"][0]["executed_suggestion_ids"] == [P_IDS[2]]


def test_license_expiry_after_partial_execution_and_substitute():
    # 组合预算先阻塞第二项；重开时它与替代项许可都已过期：
    # 过期许可不能沿用，已执行集合与支出保持不变。
    events = [
        {"at_ms": 150, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS[:2], "budget": {"cost": 1}},
        {"at_ms": 260, "action": "resume_bundle", "plan_id": "p1",
         "substitutes": {P_IDS[1]: P_IDS[2]}},
    ]
    branch = _run_plan(events, expires_after_ms=100)
    run = branch["plans"][0]
    assert run["executed_suggestion_ids"] == [P_IDS[0]]
    assert run["spent"] == {"cost": 1.0, "risk": 1.0}
    assert len(run["budget_trajectory"]) == 1
    resumed = [m for m in run["members"] if m["action"] == "resume_bundle"]
    assert resumed[0]["suggestion_id"] == P_IDS[1]
    assert resumed[0]["outcome"] == "rejected"
    assert resumed[0]["reason"] == "suggestion_expired"
    # 替代项本身许可也已过期，不能沿用过期许可。
    assert resumed[0]["substitute_suggestion_id"] == P_IDS[2]
    assert resumed[0]["substitute_reason"] == "substitute_expired"
    statuses = {s["id"]: s["status"] for s in branch["suggestions"]}
    assert statuses[P_IDS[0]] == "effective"
    assert statuses[P_IDS[1]] == "expired"
    assert statuses[P_IDS[2]] == "expired"
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == [P_IDS[0]]


def test_substitute_with_independent_approval_replaces_expired_member():
    # 替代项是另一条独立待批准建议且仍在有效期内：可以替代执行，
    # 支出计入组合；被替代成员记录原因。
    events = [
        {"at_ms": 150, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS[:2], "budget": {"cost": 2}},
        {"at_ms": 205, "action": "resume_bundle", "plan_id": "p1",
         "substitutes": {P_IDS[1]: P_IDS[2]}},
    ]
    # d1 许可到 215、替代项 d2 到 240；先让 d1 在 150 时被预算阻塞。
    branch = _run_plan(events, expires_after_ms=100)
    run = branch["plans"][0]
    # 150 时 d0、d1 都在有效期内且预算 2 足够，两项直接执行；
    # 改用预算 1 的场景验证替代路径。
    assert run["executed_suggestion_ids"] == P_IDS[:2]
    branch = _run_plan([
        {"at_ms": 150, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS[:2], "budget": {"cost": 1}},
        {"at_ms": 205, "action": "revise_budget", "plan_id": "p1",
         "budget": {"cost": 2}},
        {"at_ms": 210, "action": "resume_bundle", "plan_id": "p1",
         "substitutes": {P_IDS[1]: P_IDS[2]}},
    ], expires_after_ms=100)
    run = branch["plans"][0]
    # 210 时 d1（有效至 215）仍有效，直接执行；替代不发生。
    assert run["executed_suggestion_ids"] == P_IDS[:2]
    branch = _run_plan([
        {"at_ms": 205, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS[:2], "budget": {"cost": 2},
         "substitutes": {P_IDS[0]: P_IDS[2]}},
    ], expires_after_ms=100)
    run = branch["plans"][0]
    # 205 时 d0（有效至 200）已过期，由独立有效的 d2（有效至 240）替代；
    # d1（有效至 215）仍有效，直接执行。
    assert run["executed_suggestion_ids"] == [P_IDS[2], P_IDS[1]]
    members = {m["suggestion_id"]: m for m in run["members"]}
    assert members[P_IDS[0]]["outcome"] == "replaced"
    assert members[P_IDS[0]]["substitute_suggestion_id"] == P_IDS[2]
    assert members[P_IDS[2]]["outcome"] == "confirmed"
    assert run["spent"] == {"cost": 2.0, "risk": 2.0}


def test_duplicate_completion_notification_does_not_double_charge():
    # 重复完成通知：同一批建议再次提交/重开，已生效成员跳过，
    # 不重复执行、不重复计入预算轨迹。
    events = [
        {"at_ms": 150, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS[:2], "budget": {"cost": 2}},
        {"at_ms": 160, "action": "plan_bundle", "plan_id": "p2",
         "suggestion_ids": P_IDS[:2], "budget": {"cost": 2}},
        {"at_ms": 170, "action": "resume_bundle", "plan_id": "p1"},
    ]
    branch = _run_plan(events)
    first, second = branch["plans"]
    assert first["executed_suggestion_ids"] == P_IDS[:2]
    assert first["spent"] == {"cost": 2.0, "risk": 2.0}
    assert len(first["budget_trajectory"]) == 2
    # 第二个计划看到的都是已生效成员：跳过且零支出。
    assert second["executed_suggestion_ids"] == []
    assert second["spent"] == {"cost": 0.0, "risk": 0.0}
    assert second["budget_trajectory"] == []
    assert all(m["outcome"] == "skipped" and m["reason"] == "already_effective"
               for m in second["members"])
    # 重开第一个计划：剩余集合为空，预算轨迹不变。
    assert first["spent"] == {"cost": 2.0, "risk": 2.0}
    assert len(first["budget_trajectory"]) == 2
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == P_IDS[:2]


def test_manual_stop_budget_revision_and_resume(tmp_path: Path):
    # 人工停止保留已执行集合与未执行原因；预算突增（修订）后重开，
    # 只继续经重新核对的剩余部分；保存重放结果一致。
    scenario = _plan_scenario()
    thresholds = dict(_APPROVAL_THRESHOLDS)
    events = [
        {"at_ms": 150, "action": "plan_bundle", "plan_id": "p1",
         "suggestion_ids": P_IDS, "budget": {"cost": 1}},
        {"at_ms": 160, "action": "stop_bundle", "plan_id": "p1"},
        {"at_ms": 165, "action": "revise_budget", "plan_id": "p1",
         "budget": {"cost": 3}},
        {"at_ms": 170, "action": "resume_bundle", "plan_id": "p1"},
    ]
    runner = ExperimentRunner(_FailingChain())
    branch = runner.run_adaptive_branch(scenario, thresholds, approvals=events)
    run = branch["plans"][0]
    # 初始：d0 执行，d1 因组合预算被阻塞，d2 随计划停止而暂缓；
    # 预算修订后重开，d1、d2 经重新核对继续执行。
    assert run["executed_suggestion_ids"] == P_IDS
    reasons = {}
    for note in run["not_executed"]:
        reasons.setdefault(note["suggestion_id"], []).append(note["reason"])
    assert "plan_budget_exceeded" in reasons[P_IDS[1]]
    assert "manual_stop" in reasons[P_IDS[2]]
    # 预算修订事件记录了修订时点的永久支出。
    revise = next(e for e in run["events"] if e["action"] == "revise_budget")
    assert revise["spent_at_revision"] == {"cost": 1.0, "risk": 1.0}
    assert revise["new_budget"] == {"cost": 3.0, "risk": None}
    # 已执行支出永久保留：重开只追加，不回溯。
    assert run["spent"] == {"cost": 3.0, "risk": 3.0}
    assert [t["suggestion_id"] for t in run["budget_trajectory"]] == [
        P_IDS[0], P_IDS[1], P_IDS[2]]
    assert run["status"] == "effective"
    starts = [e["id"] for e in branch["fault_events"]
              if e["event"] == "inject_start"]
    assert starts == P_IDS
    # 保存重放一致。
    experiment = {"scenario_name": scenario.name, "seed": scenario.seed,
                  "branches": [branch]}
    path = tmp_path / "plan.json"
    save_adaptive_run(experiment, scenario, path)
    saved = load_run(path)
    assert saved["result"]["branches"][0]["plans"] == branch["plans"]
    replayed = runner.run_adaptive_branch(
        scenario_from_dict(saved["scenario"]), thresholds,
        approvals=saved["result"]["branches"][0]["approvals"])
    assert replayed["plans"] == branch["plans"]
