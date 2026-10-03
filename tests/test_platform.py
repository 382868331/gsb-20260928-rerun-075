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


def _approval_scenario(limits=None, expires_after_ms=200, allow_overlap=False):
    policy = {
        "decision_at_ms": [100, 110],
        "limits": limits or {},
        "approval": {
            "required": True,
            "expires_after_ms": expires_after_ms,
            "allow_overlap": allow_overlap,
        },
        "actions": {
            "failed": {
                "type": "interruption", "target": "inventory",
                "duration_ms": 100, "params": {}, "cost": 1, "risk": 1,
            },
        },
    }
    return Scenario(
        name="approval", seed=7,
        requests=[RequestSpec(50, {"i": 0}), RequestSpec(90, {"i": 1})],
        adaptive_policy=policy,
    )


class _FailingChain:
    def call(self, request_id, payload, seed, manager):
        return CallResult(request_id, payload, "interrupted", 20_000_000,
                          failed_node="inventory", error="stub failure")


_APPROVAL_THRESHOLDS = {"window_ms": 30, "min_observations": 1,
                        "failure_rate": 1.0, "avg_latency_ms": 100000}


def _run_approval(approvals, **kwargs):
    scenario = _approval_scenario(**kwargs)
    return ExperimentRunner(_FailingChain()).run_adaptive_branch(
        scenario, dict(_APPROVAL_THRESHOLDS), approvals=approvals)


def test_approval_suggestion_expires_without_confirmation():
    # 操作者迟迟未确认：建议过期，不能生效，也不算已发生的故障注入。
    branch = _run_approval(
        [{"decision_index": 0, "at_ms": 400, "action": "confirm"}],
        expires_after_ms=100)
    first, second = branch["suggestions"]
    assert first["status"] == "expired"
    assert first["expired_at_ms"] == 200
    assert first["events"][-1]["outcome"] == "rejected"
    assert first["events"][-1]["reason"] == "suggestion_expired"
    assert second["status"] == "expired"
    # 未获批准的建议不出现在故障事件里，对照比较无任何注入影响
    assert branch["fault_events"] == []
    assert all(not c["status_changed"] for c in branch["comparisons"])
    assert branch["stopped"] is False


def test_approval_confirm_rechecks_risk_budget_at_confirm_time():
    # 风险上限先触发：第一条确认占满风险余额，第二条确认时被拒绝并停止。
    branch = _run_approval(
        [{"decision_index": 0, "at_ms": 120, "action": "confirm"},
         {"decision_index": 1, "at_ms": 130, "action": "confirm"}],
        limits={"max_risk": 1})
    first, second = branch["suggestions"]
    assert first["status"] == "effective"
    assert first["events"][0]["budget_after"]["risk"] == 1
    assert second["status"] == "pending"
    assert second["events"][0]["outcome"] == "rejected"
    assert second["events"][0]["reason"] == "risk_limit_reached"
    assert branch["stopped"] is True
    assert branch["stop_reason"] == "risk_limit_reached"
    started = [e for e in branch["fault_events"] if e["event"] == "inject_start"]
    assert [e["id"] for e in started] == [first["id"]]


def test_approval_scope_contention_waits_for_previous_to_end():
    # 两项建议争用同一范围：必须等待前一项结束才能确认。
    branch = _run_approval(
        [{"decision_index": 0, "at_ms": 120, "action": "confirm"},
         {"decision_index": 1, "at_ms": 130, "action": "confirm"},
         {"decision_index": 1, "at_ms": 230, "action": "confirm"}])
    first, second = branch["suggestions"]
    assert first["status"] == "effective"
    deferred, confirmed = second["events"]
    assert deferred["outcome"] == "deferred"
    assert deferred["reason"] == "scope_contention"
    assert deferred["conflicting_intervention_ids"] == [first["id"]]
    # 前一项 120+100=220 结束，230 重试确认成功
    assert confirmed["outcome"] == "confirmed"
    assert second["status"] == "effective"
    starts = [e for e in branch["fault_events"] if e["event"] == "inject_start"]
    assert [e["at_ms"] for e in starts] == [120, 230]
    # 两个建议的确认依据（决策时窗口）不被迟到观察改写
    assert first["basis"]["window_observation_ids"] == [0]
    assert second["basis"]["window_observation_ids"] == [1]


def test_approval_withdraw_then_reopen_same_scope():
    # 收尾期间重开：撤回已生效干预后，同一范围的新建议可以确认。
    branch = _run_approval(
        [{"decision_index": 0, "at_ms": 120, "action": "confirm"},
         {"decision_index": 0, "at_ms": 140, "action": "withdraw"},
         {"decision_index": 1, "at_ms": 150, "action": "confirm"}])
    first, second = branch["suggestions"]
    assert first["status"] == "withdrawn"
    assert first["withdrawn_at_ms"] == 140
    assert second["status"] == "effective"
    # 撤回按原收尾约定记录 inject_end
    ends = [e for e in branch["fault_events"]
            if e["event"] == "inject_end" and e["id"] == first["id"]]
    assert [e["at_ms"] for e in ends] == [140]
    # 记录每条实际生效干预影响了哪些观察
    assert first["affected_observation_ids"] == []
    assert second["affected_observation_ids"] == []


def test_approval_abandon_keeps_history_and_control_independent():
    branch = _run_approval(
        [{"decision_index": 0, "at_ms": 120, "action": "abandon"},
         {"decision_index": 1, "at_ms": 150, "action": "confirm"}])
    first, second = branch["suggestions"]
    assert first["status"] == "abandoned"
    assert first["abandoned_at_ms"] == 120
    assert second["status"] == "effective"
    # 放弃不改写历史观察；对照组独立且不含任何建议效果
    assert len(branch["control_observations"]) == 2
    assert all(not o["active_intervention_ids"]
               for o in branch["control_observations"])
    started = [e["id"] for e in branch["fault_events"]
               if e["event"] == "inject_start"]
    assert started == [second["id"]]


def test_approval_run_saves_and_replays(tmp_path: Path):
    scenario = _approval_scenario()
    approvals = [{"decision_index": 0, "at_ms": 120, "action": "confirm"}]
    runner = ExperimentRunner(_FailingChain())
    result = runner.run_adaptive_experiment(
        scenario, dict(_APPROVAL_THRESHOLDS), approvals=approvals)
    path = tmp_path / "approval.json"
    save_adaptive_run(result, scenario, path)
    saved = load_run(path)
    replayed = runner.run_adaptive_experiment(
        scenario_from_dict(saved["scenario"]), dict(_APPROVAL_THRESHOLDS),
        approvals=saved["result"]["branches"][0]["approvals"])
    assert replayed == saved["result"]
