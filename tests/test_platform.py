from pathlib import Path

import pytest

from chaos_platform import (
    EffectHandle,
    ExperimentRunner,
    FaultManager,
    InjectionSpec,
    RequestSpec,
    Scenario,
    fault_registry,
    load_scenario,
    save_run,
    save_adaptive_run,
    load_run,
)
from chaos_platform.service_chain import CallResult
from chaos_platform.cli import main as cli_main
from chaos_platform.scenario import scenario_from_dict


def _scenario():
    return Scenario(
        name="t",
        seed=42,
        requests=[RequestSpec(at_ms=t, payload={"i": i})
                  for i, t in enumerate((0, 50, 150, 250, 350))],
        injections=[
            InjectionSpec("a", "latency", "billing", 20, 300, {"delay_ms": 50}),
            InjectionSpec("b", "latency", "billing", 100, 200, {"delay_ms": 30}),
            InjectionSpec("c", "interruption", "inventory", 120, 180),
        ],
    )


def test_overlapping_latency_ends_independently():
    manager = FaultManager()
    manager.start(InjectionSpec("a", "latency", "billing", 0, None,
                                {"delay_ms": 50}), 0)
    manager.start(InjectionSpec("b", "latency", "billing", 0, None,
                                {"delay_ms": 30}), 0)
    assert manager.added_latency_ns("billing") == 80_000_000
    manager.end("b", 10)
    # b 结束不能误撤 a
    assert manager.active_ids() == ["a"]
    assert manager.added_latency_ns("billing") == 50_000_000
    manager.end("a", 20)
    assert manager.is_empty()


def test_run_compares_real_request_results():
    result = ExperimentRunner().run(_scenario())
    by_request = {c.request_id: c for c in result.comparisons}
    # 请求 @150 落在中断窗口内：真实结果从 ok 变为 interrupted
    assert by_request[2].status_changed is True
    assert by_request[2].fault_status == "interrupted"
    assert by_request[2].failed_node == "inventory"
    # 请求 @50 仅命中延迟 a
    assert by_request[1].latency_delta_ns == 50_000_000
    # 请求 @150 命中 a+b 后在 inventory 被中断；核对实际经过的 billing 跳
    obs = {o.request_id: o for o in result.observations if o.group == "fault"}
    base_obs = {o.request_id: o for o in result.observations if o.group == "control"}
    fault_billing = next(h["latency_ns"] for h in obs[2].result["hops"]
                         if h["node"] == "billing")
    base_billing = next(h["latency_ns"] for h in base_obs[2].result["hops"]
                        if h["node"] == "billing")
    assert fault_billing - base_billing == 80_000_000
    # 窗口外的请求与对照组完全一致
    assert by_request[0].latency_delta_ns == 0
    assert by_request[4].status_changed is False


def test_cancel_revokes_still_active_faults():
    scenario = Scenario(
        name="cancel", seed=1,
        requests=[RequestSpec(50), RequestSpec(300)],
        injections=[
            InjectionSpec("lat", "latency", "auth", 20, None, {"delay_ms": 10}),
            InjectionSpec("brk", "interruption", "billing", 30, None),
        ],
        cancel_at_ms=100,
    )
    result = ExperimentRunner().run(scenario)
    assert result.cancelled is True
    assert {r["id"] for r in result.recovered} == {"lat", "brk"}
    # 取消后的请求与对照组一致，说明故障确实被撤销
    after = {c.request_id: c for c in result.comparisons}[1]
    assert after.status_changed is False
    assert after.latency_delta_ns == 0


def test_seed_replay_is_byte_identical(tmp_path: Path):
    scenario = _scenario()
    runner = ExperimentRunner()
    first = runner.run(scenario).to_dict()
    second = runner.run(scenario).to_dict()
    assert first == second
    report = tmp_path / "run.json"
    save_run(runner.run(scenario), scenario, report)
    loaded = load_run(report)
    assert ExperimentRunner().run(scenario_from_dict(loaded["scenario"])).to_dict() \
        == loaded["result"]
    assert cli_main(["replay", str(report)]) == 0


def test_new_fault_type_without_touching_runner():
    class ErrorFault:
        type_name = "forced_error"

        def create_handle(self, spec):
            return EffectHandle(injection_id=spec.id, type_name=self.type_name,
                                target=spec.target, aspect="forced_error")

    fault_registry.register(ErrorFault())
    try:
        spec = InjectionSpec("e", "forced_error", "billing", 0, 10)
        manager = FaultManager()
        manager.start(spec, 0)
        assert manager.active_ids() == ["e"]
        manager.end("e", 10)
        assert manager.is_empty()
    finally:
        fault_registry._types.pop("forced_error", None)


def test_example_scenarios_run_and_replay(tmp_path: Path):
    examples = Path(__file__).resolve().parents[1] / "examples"
    scenario = load_scenario(examples / "first_round.json")
    result = ExperimentRunner().run(scenario)
    assert result.comparisons
    report = tmp_path / "r.json"
    assert cli_main(["run", str(examples / "cancel_demo.json"), "-o", str(report)]) == 0
    assert cli_main(["replay", str(report)]) == 0


def test_adaptive_decisions_are_immutable_deduplicated_and_bounded():
    policy = {
        "decision_at_ms": [150, 200, 300, 400, 440],
        "limits": {"max_interventions": 1, "max_cost": 10, "max_risk": 10},
        "actions": {
            "failed": {
                "type": "interruption",
                "target": "notify",
                "duration_ms": 50,
                "params": {},
                "cost": 1,
                "risk": 1,
            }
        },
    }
    scenario = Scenario(
        name="adaptive",
        seed=9,
        requests=[
            RequestSpec(80, {"n": 0}),
            RequestSpec(130, {"n": 1}),
            RequestSpec(145, {"n": 2}),
            RequestSpec(380, {"n": 3}),
        ],
        adaptive_policy=policy,
    )
    thresholds = {
        "window_ms": 45,
        "min_observations": 1,
        "failure_rate": 1.0,
        "avg_latency_ms": 100000,
    }
    class FailingChain:
        def call(self, request_id, payload, seed, manager):
            if request_id in (1, 2, 3):
                return CallResult(request_id, payload, "interrupted", 20_000_000,
                                  failed_node="inventory",
                                  error="stub inventory failure")
            return CallResult(request_id, payload, "ok", 20_000_000)

    result = ExperimentRunner(FailingChain()).run_adaptive_branch(
        scenario, thresholds)
    decisions = result["decisions"]

    # 请求 0 虽已到达但不在窗口；请求 2 调度于 145，175 才迟到。
    assert decisions[0]["arrived_observation_ids"] == [0, 1]
    assert decisions[0]["window_observation_ids"] == [1]
    assert decisions[0]["new_failure_ids"] == [1]
    assert decisions[0]["action"]["selected"] is True
    assert decisions[0]["thresholds"] == thresholds

    # 200 时请求 2 已到；它不能改写 150，只会在当前点受预算阻止。
    assert decisions[1]["new_failure_ids"] == [2]
    assert decisions[1]["failed_observation_ids"] == [2]
    assert decisions[1]["action"]["selected"] is False
    assert decisions[1]["action"]["skip_reason"] == "max_interventions_reached"
    assert result["stopped"] is True
    assert result["stop_reason"] == "max_interventions_reached"

    assert decisions[2]["severity"] == "undetermined"
    assert decisions[2]["action"] is None
    # 同一新失败落在 355~400 窗口，但预算已停，不能创建干预。
    assert decisions[3]["failed_observation_ids"] == [3]
    assert decisions[3]["new_failure_ids"] == [3]
    assert decisions[3]["already_escalated_failure_ids"] == []
    assert decisions[3]["action"]["selected"] is False
    assert decisions[3]["action"]["skip_reason"] == "max_interventions_reached"
    # 395~440 与上一窗口重叠；同一失败只能作为已知失败，不能再次推动升级。
    assert decisions[4]["failed_observation_ids"] == [3]
    assert decisions[4]["new_failure_ids"] == []
    assert decisions[4]["already_escalated_failure_ids"] == [3]
    assert decisions[4]["action"] is None

    # 停止前选择的干预按 duration_ms 自行结束，不是在停止点立即撤销。
    events = result["fault_events"]
    start = next(e for e in events if e["event"] == "inject_start"
                 and e["id"] == "root:d0:failed")
    end = next(e for e in events if e["event"] == "inject_end"
               and e["id"] == "root:d0:failed")
    assert start["at_ms"] == 150
    assert end["at_ms"] == 200


def test_adaptive_insufficient_data_stays_undetermined():
    policy = {
        "decision_at_ms": [100],
        "limits": {},
        "actions": {
            "degraded": {
                "type": "latency", "target": "billing",
                "duration_ms": 10, "params": {"delay_ms": 1},
            }
        },
    }
    scenario = Scenario(
        name="undetermined", seed=1,
        requests=[RequestSpec(80), RequestSpec(90)],
        adaptive_policy=policy,
    )
    result = ExperimentRunner().run_adaptive_branch(
        scenario, {"window_ms": 100, "min_observations": 3,
                    "avg_latency_ms": 0})
    assert result["decisions"][0]["severity"] == "undetermined"
    assert result["decisions"][0]["action"] is None
    assert result["fault_events"] == []


def test_alternate_threshold_branch_is_independent_and_replays(tmp_path: Path):
    policy = {
        "decision_at_ms": [100],
        "limits": {"max_interventions": 1},
        "actions": {
            "degraded": {
                "type": "interruption", "target": "notify",
                "duration_ms": 20, "params": {},
                "cost": 1, "risk": 1,
            }
        },
    }
    scenario = Scenario(
        name="branch", seed=2,
        requests=[RequestSpec(20, {"x": 1})],
        injections=[InjectionSpec("slow", "latency", "billing", 0, 200,
                                  {"delay_ms": 30})],
        adaptive_policy=policy,
    )
    root_thresholds = {"window_ms": 100, "min_observations": 1,
                       "failure_rate": 1, "avg_latency_ms": 1000000}
    alt_thresholds = {"window_ms": 100, "min_observations": 1,
                      "failure_rate": 1, "avg_latency_ms": 1}
    class SlowChain:
        def call(self, request_id, payload, seed, manager):
            base_latency = 50_000_000 if request_id == 0 else 20_000_000
            if manager is None:
                return CallResult(request_id, payload, "ok", base_latency)
            if manager.interruption_active("billing"):
                return CallResult(request_id, payload, "interrupted", base_latency,
                                  failed_node="billing")
            latency = base_latency + manager.added_latency_ns("billing")
            return CallResult(request_id, payload, "ok", latency)

    experiment = ExperimentRunner(SlowChain()).run_adaptive_experiment(
        scenario, root_thresholds, alt_thresholds)
    root, alternate = experiment["branches"]

    assert root["decisions"][0]["severity"] == "healthy"
    assert root["decisions"][0]["action"] is None
    assert alternate["decisions"][0]["severity"] == "degraded"
    assert alternate["decisions"][0]["action"]["selected"] is True
    assert root["thresholds"] != alternate["thresholds"]
    assert alternate["control_observations"][0]["result"] == \
        root["control_observations"][0]["result"]
    assert alternate["observations"] != root["observations"]
    assert alternate["decisions"][0]["action"]["spec"]["id"].startswith(
        "alternate:")
    assert {e["id"] for e in root["fault_events"]} == {"slow"}
    assert {e["id"] for e in alternate["fault_events"]} == {
        "slow", "alternate:d0:degraded"}

    save_adaptive_run(experiment, scenario, tmp_path / "adaptive.json")
    replayed = ExperimentRunner(SlowChain()).run_adaptive_experiment(
        scenario, root_thresholds, alt_thresholds)
    assert replayed == experiment


class _FailingChain:
    def __init__(self, failing):
        self.failing = set(failing)

    def call(self, request_id, payload, seed, manager):
        if request_id in self.failing:
            return CallResult(request_id, payload, "interrupted", 20_000_000,
                              failed_node="inventory",
                              error="stub inventory failure")
        return CallResult(request_id, payload, "ok", 20_000_000)


_APPROVAL_THRESHOLDS = {"window_ms": 45, "min_observations": 1,
                        "failure_rate": 1.0, "avg_latency_ms": 100000}


def _approval_scenario(request_times, approvals, expires_after_ms=200,
                       overlap="wait", max_risk=10, duration_ms=100):
    policy = {
        "decision_at_ms": [150, 250],
        "limits": {"max_interventions": 5, "max_cost": 10,
                   "max_risk": max_risk},
        "approval": {
            "required": True,
            "expires_after_ms": expires_after_ms,
            "overlap": overlap,
            "decisions": approvals,
        },
        "actions": {
            "failed": {
                "type": "interruption",
                "target": "inventory",
                "duration_ms": duration_ms,
                "params": {},
                "cost": 1,
                "risk": 1,
            }
        },
    }
    return Scenario(
        name="approval", seed=7,
        requests=[RequestSpec(t, {"n": i}) for i, t in enumerate(request_times)],
        adaptive_policy=policy,
    )


def _suggestion(branch, suggestion_id):
    return next(s for s in branch["suggestions"] if s["id"] == suggestion_id)


def test_approval_never_confirmed_suggestion_expires_without_injection():
    scenario = _approval_scenario([80, 130, 400], approvals=[],
                                  expires_after_ms=100)
    branch = ExperimentRunner(_FailingChain({1})).run_adaptive_branch(
        scenario, _APPROVAL_THRESHOLDS)

    suggestion = _suggestion(branch, "root:d0:failed")
    # 操作者迟迟未确认：过期后建议作废，且从不算作故障注入。
    assert suggestion["status"] == "expired"
    assert suggestion["expires_at_ms"] == 250
    assert branch["fault_events"] == []
    assert branch["approvals"] == []
    # 治疗组观察与对照组完全一致，对照组独立保留。
    assert all(not c["status_changed"] and c["latency_delta_ns"] == 0
               for c in branch["comparisons"])
    assert [o["result"] for o in branch["observations"]] == \
        [o["result"] for o in branch["control_observations"]]


def test_approval_risk_limit_then_stop_condition_blocks_confirmation():
    approvals = [
        {"at_ms": 300, "suggestion": "root:d0:failed", "choice": "confirm"},
        {"at_ms": 310, "suggestion": "root:d1:failed", "choice": "confirm"},
        {"at_ms": 320, "suggestion": "root:d1:failed", "choice": "confirm"},
    ]
    scenario = _approval_scenario([80, 130, 230], approvals=approvals,
                                  expires_after_ms=500, max_risk=1)
    branch = ExperimentRunner(_FailingChain({1, 2})).run_adaptive_branch(
        scenario, _APPROVAL_THRESHOLDS)

    outcomes = [(a["choice"], a["outcome"], a["reason"])
                for a in branch["approvals"]]
    assert outcomes == [
        ("confirm", "confirmed", None),
        # 风险上限在确认时重检：1 + 1 > max_risk=1，触发停止。
        ("confirm", "rejected", "risk_limit_reached"),
        # 已触发的停止条件不能被后续确认绕过。
        ("confirm", "rejected", "stop_condition_active"),
    ]
    assert branch["stopped"] is True
    assert branch["stop_reason"] == "risk_limit_reached"
    assert _suggestion(branch, "root:d0:failed")["status"] == "completed"
    assert _suggestion(branch, "root:d1:failed")["status"] == "unconfirmed"
    # 只有 d0 真正生效过。
    assert {e["id"] for e in branch["fault_events"]} == {"root:d0:failed"}


def test_approval_contention_wait_then_reopen_and_withdraw():
    approvals = [
        {"at_ms": 160, "suggestion": "root:d0:failed", "choice": "confirm"},
        {"at_ms": 255, "suggestion": "root:d1:failed", "choice": "confirm"},
        {"at_ms": 265, "suggestion": "root:d1:failed", "choice": "confirm"},
        {"at_ms": 300, "suggestion": "root:d1:failed", "choice": "withdraw"},
    ]
    scenario = _approval_scenario([80, 130, 230, 280, 350],
                                  approvals=approvals)
    branch = ExperimentRunner(_FailingChain({1, 2})).run_adaptive_branch(
        scenario, _APPROVAL_THRESHOLDS)

    outcomes = [(a["at_ms"], a["outcome"], a["reason"])
                for a in branch["approvals"]]
    assert outcomes == [
        (160, "confirmed", None),
        # d0 生效到 260，d1 争用同一 (interruption, inventory)，必须等待。
        (255, "rejected", "scope_contention"),
        # 前一项收尾结束后重开成功。
        (265, "confirmed", None),
        (300, "withdrawn", None),
    ]
    contention = branch["approvals"][1]["recheck"]
    assert contention["can_coexist"] is False
    assert contention["conflicting_intervention_ids"] == ["root:d0:failed"]
    assert contention["active_intervention_ids"] == ["root:d0:failed"]

    events = [(e["event"], e["id"], e["at_ms"])
              for e in branch["fault_events"]]
    assert events == [
        ("inject_start", "root:d0:failed", 160),
        ("inject_end", "root:d0:failed", 260),
        ("inject_start", "root:d1:failed", 265),
        # 撤回按原有收尾约定立即撤销。
        ("inject_end", "root:d1:failed", 300),
    ]

    d0 = _suggestion(branch, "root:d0:failed")
    d1 = _suggestion(branch, "root:d1:failed")
    assert d0["status"] == "completed"
    assert d0["confirmed_at_ms"] == 160
    assert d0["affected_observation_ids"] == [2]
    assert d1["status"] == "withdrawn"
    assert d1["withdrawn_at_ms"] == 300
    assert d1["effective_spec"]["start_ms"] == 265
    # 请求 3 在 d1 生效期间到达；撤回后的到达结果被逐条记录。
    assert d1["affected_observation_ids"] == [3]
    assert d1["post_withdrawal_observation_ids"] == [3, 4]


def test_approval_abandon_and_late_observation_does_not_rewrite_basis():
    # 请求 2 调度于 145，165 才到达：建议于 150 生成时它尚未到达。
    scenario = _approval_scenario(
        [80, 130, 145, 380],
        approvals=[{"at_ms": 170, "suggestion": "root:d0:failed",
                    "choice": "abandon"}])
    branch = ExperimentRunner(_FailingChain({1, 2})).run_adaptive_branch(
        scenario, _APPROVAL_THRESHOLDS)

    suggestion = _suggestion(branch, "root:d0:failed")
    assert suggestion["status"] == "abandoned"
    # 放弃未生效建议不改变历史观察，也未产生任何注入。
    assert branch["fault_events"] == []
    assert len(branch["observations"]) == 4
    # 迟到观察不能改写原确认依据。
    assert suggestion["basis"]["arrived_observation_ids"] == [0, 1]
    assert branch["approvals"][0]["outcome"] == "abandoned"

    # 换成确认：迟到观察只出现在确认时的重检里，依据快照保持原样。
    scenario = _approval_scenario(
        [80, 130, 145, 380],
        approvals=[{"at_ms": 170, "suggestion": "root:d0:failed",
                    "choice": "confirm"}])
    branch = ExperimentRunner(_FailingChain({1, 2})).run_adaptive_branch(
        scenario, _APPROVAL_THRESHOLDS)
    suggestion = _suggestion(branch, "root:d0:failed")
    approval = branch["approvals"][0]
    assert approval["outcome"] == "confirmed"
    assert approval["recheck"]["arrived_observation_ids"] == [0, 1, 2]
    assert approval["recheck"]["basis_intact"] is True
    assert suggestion["basis"]["arrived_observation_ids"] == [0, 1]
    assert suggestion["status"] == "completed"

    # 确定性：相同输入重跑结果一致。
    again = ExperimentRunner(_FailingChain({1, 2})).run_adaptive_branch(
        scenario, _APPROVAL_THRESHOLDS)
    assert again == branch
