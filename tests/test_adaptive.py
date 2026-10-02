import json

from chaos_platform.adaptive import (
    AdaptiveRunner,
    explain_branch,
    load_adaptive_run,
    policy_from_dict,
    replay_adaptive,
    run_branch,
    save_adaptive_run,
)
from chaos_platform.cli import main as cli_main
from chaos_platform.scenario import scenario_from_dict


def _scenario_dict(**policy_overrides):
    policy = {
        "window_ms": 120,
        "min_samples": 2,
        "observation_delay_ms": 0,
        "thresholds": {"failure_rate": 0.5, "latency_delta_ms": 30},
        "ladder": [
            {"type_name": "latency", "target": "auth", "duration_ms": 150,
             "params": {"delay_ms": 20}},
            {"type_name": "interruption", "target": "billing", "duration_ms": 100},
            {"type_name": "interruption", "target": "inventory", "duration_ms": 100},
        ],
        "budget": {"max_interventions": 2, "max_failed_requests": 4},
    }
    policy.update(policy_overrides)
    return {
        "name": "t",
        "seed": 7,
        "requests": [{"at_ms": t, "payload": {"i": i}}
                     for i, t in enumerate(range(0, 600, 50))],
        "injections": [
            {"id": "bg", "type": "latency", "target": "billing",
             "start_ms": 0, "end_ms": 600, "params": {"delay_ms": 40}},
        ],
        "policy": policy,
    }


def _run(**policy_overrides):
    data = _scenario_dict(**policy_overrides)
    scenario = scenario_from_dict(data)
    policy = policy_from_dict(data["policy"])
    return AdaptiveRunner().run(scenario, policy), scenario


def test_escalation_records_observations_thresholds_and_action():
    result, _ = _run()
    escalations = [d for d in result.decisions if d.action == "escalate"]
    assert escalations, "背景延迟越限应推动升级"
    first = escalations[0]
    # 决策快照包含当时已到达的观察、阈值与所选动作
    assert first.observation_ids
    assert first.thresholds["latency_delta_ms"] == 30
    assert first.injection_id is not None and first.rung == 0
    # 阶梯按顺序推进
    assert [d.rung for d in escalations] == list(range(len(escalations)))


def test_insufficient_data_marked_undetermined():
    result, _ = _run(min_samples=3, window_ms=10)
    assert result.decisions
    assert all(d.action == "undetermined" for d in result.decisions)
    assert all(d.reason == "insufficient_samples" for d in result.decisions)


def test_late_observations_do_not_rewrite_earlier_decisions():
    result, _ = _run(observation_delay_ms=500)
    # 所有观察都迟到：前面的决策只能基于当时已到达的观察（空窗口）
    first = result.decisions[0]
    assert first.observation_ids == []
    assert first.action == "undetermined"
    # 决策记录保存后不因后续观察到达而改变
    replayed, _ = _run(observation_delay_ms=500)
    assert [d.to_dict() for d in replayed.decisions] == \
        [d.to_dict() for d in result.decisions]


def test_budget_stops_new_interventions_but_active_effects_finish():
    result, _ = _run(budget={"max_interventions": 1, "max_failed_requests": 99})
    assert result.stopped
    assert result.stop_reason == "budget_exhausted"
    starts = [e for e in result.fault_events if e["event"] == "inject_start"
              and e["id"].startswith("auto-")]
    assert len(starts) == 1  # 达到预算后不再创建新干预
    # 已生效的干预按自己的 duration 结束，现场归零
    assert not any(e["event"] == "recover" for e in result.fault_events)
    stops = [d for d in result.decisions if d.action == "stop"]
    assert stops and stops[0].reason == "budget_exhausted"


def test_overlapping_windows_do_not_reuse_same_failure():
    # 窗口 120ms 与请求间隔 50ms 高度重叠；中断源持续存在
    data = _scenario_dict()
    data["injections"] = [
        {"id": "bg-break", "type": "interruption", "target": "billing",
         "start_ms": 0, "end_ms": 600},
    ]
    data["policy"]["thresholds"] = {"failure_rate": 0.5, "latency_delta_ms": None}
    data["policy"]["budget"] = {"max_interventions": 10, "max_failed_requests": 99}
    scenario = scenario_from_dict(data)
    policy = policy_from_dict(data["policy"])
    result = AdaptiveRunner().run(scenario, policy)
    escalations = [d for d in result.decisions if d.action == "escalate"]
    # 阶梯只有 3 级：若同一失败可重复推动，升级次数会远超阶梯长度
    assert len(escalations) <= len(policy.ladder)
    # 每次升级都由此前未消费的新失败推动；同一失败不会重复推动升级
    consumed: set[int] = set()
    for d in escalations:
        window_failures = {
            obs.request_id for obs in result.observations
            if obs.request_id in d.observation_ids
            and obs.result["status"] != "ok"
        }
        fresh = window_failures - consumed
        assert fresh, "升级必须由新失败推动"
        assert len(fresh) == d.metrics["new_failures"]
        consumed |= window_failures


def test_control_process_present_for_recovery_attribution():
    result, _ = _run()
    assert result.control_observations
    assert all(o.group == "control" for o in result.control_observations)
    assert len(result.comparisons) == len(result.observations)


def test_save_replay_and_branch(tmp_path):
    result, scenario = _run()
    report = tmp_path / "run.json"
    save_adaptive_run(result, scenario, report)
    saved = load_adaptive_run(report)

    # 逐步复演原决策，逐字段一致
    assert replay_adaptive(saved).to_dict() == saved["result"]

    # 另一套阈值建立独立分支，原报告不被覆盖
    before = report.read_text(encoding="utf-8")
    branch_result, branch_scenario = run_branch(saved, {"latency_delta_ms": 1000})
    branch_path = tmp_path / "branch.json"
    save_adaptive_run(branch_result, branch_scenario, branch_path,
                      kind="adaptive_branch",
                      extra={"threshold_overrides": {"latency_delta_ms": 1000}})
    assert report.read_text(encoding="utf-8") == before

    branch_saved = load_adaptive_run(branch_path)
    explanations = explain_branch(saved, branch_saved)
    assert explanations, "阈值大幅放宽后决策应出现分歧"
    first = explanations[0]
    assert first["cause"] == "threshold_change"
    assert first["threshold_changes"][0]["threshold"] == "latency_delta_ms"
    # 后续分歧标记为上游分歧的下游影响
    for entry in explanations[1:]:
        assert entry["cause"] == "downstream_of_divergence"


def test_cli_adaptive_roundtrip(tmp_path, capsys):
    scenario_path = tmp_path / "scenario.json"
    scenario_path.write_text(json.dumps(_scenario_dict()), encoding="utf-8")
    report = tmp_path / "report.json"
    assert cli_main(["adaptive", "run", str(scenario_path),
                     "-o", str(report)]) == 0
    assert cli_main(["adaptive", "replay", str(report)]) == 0
    branch = tmp_path / "branch.json"
    assert cli_main(["adaptive", "branch", str(report),
                     "--set", "failure_rate=0.9", "-o", str(branch)]) == 0
    assert cli_main(["adaptive", "explain", str(report), str(branch)]) == 0
