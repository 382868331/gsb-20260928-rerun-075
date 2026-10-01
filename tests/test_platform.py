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
    load_run,
)
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


def _dependency_scenario():
    return Scenario(
        name="dep", seed=7,
        requests=[RequestSpec(at_ms=t, retries=2)
                  for t in (50, 150, 250, 600)],
        injections=[
            InjectionSpec("lat", "latency", "billing", 100, 500,
                          {"delay_ms": 80}),
            InjectionSpec("tmo", "timeout", "billing", 100, 500,
                          {"threshold_ms": 50},
                          depends_on=["lat"],
                          start_when={"kind": "latency_at_least",
                                      "target": "billing", "ms": 60}),
        ],
    )


def test_dependent_timeout_fires_only_after_observed_latency():
    result = ExperimentRunner().run(_dependency_scenario())
    events = result.event_log
    fired = next(e for e in events
                 if e["kind"] == "inject_start" and e.get("id") == "tmo")
    # 超时注入不是在 start_ms=100 生效，而是在第一次观察到 billing 延迟超标后
    assert fired["at_ms"] == 150
    assert fired["conditional"] is True
    # 生效前（@50/@150）请求与对照组一致；生效后（@250）超时且重试也失败
    by_request = {c.request_id: c for c in result.comparisons}
    assert by_request[0].status_changed is False
    assert by_request[1].status_changed is False
    assert by_request[2].fault_status == "timeout"
    obs = {o.request_id: o for o in result.observations if o.group == "fault"}
    assert len(obs[2].attempts) == 3  # 1 次原始 + 2 次重试，全部超时
    assert all(a["status"] == "timeout" for a in obs[2].attempts)
    # 延迟结束后（@600）超时注入也随窗口结束，请求恢复
    assert by_request[3].status_changed is False


def test_impact_attributed_to_source_injections():
    result = ExperimentRunner().run(_dependency_scenario())
    by_request = {c.request_id: c for c in result.comparisons}
    # 超时请求的最终影响可追溯到 lat（延迟贡献）与 tmo（超时判定）两次注入
    assert by_request[2].attributed_to == ["lat", "tmo"]
    assert by_request[2].failed_node == "billing"
    # 纯延迟场景只归因于延迟注入
    simple = Scenario(
        name="s", seed=3,
        requests=[RequestSpec(at_ms=50)],
        injections=[InjectionSpec("only-lat", "latency", "auth", 10, 100,
                                  {"delay_ms": 20})],
    )
    comp = ExperimentRunner().run(simple).comparisons[0]
    assert comp.attributed_to == ["only-lat"]
    assert comp.latency_delta_ns == 20_000_000


def test_early_stop_skips_later_requests_and_recovers():
    scenario = Scenario(
        name="stop", seed=5,
        requests=[RequestSpec(at_ms=t) for t in (50, 150, 250, 350, 450)],
        injections=[InjectionSpec("brk", "interruption", "auth", 100, None)],
        stop_when=[{"kind": "consecutive_failures", "count": 2}],
    )
    result = ExperimentRunner().run(scenario)
    assert result.stopped is True
    assert result.stopped_at_ms == 250
    assert "consecutive_failures" in result.stop_reason
    # 停止时撤销仍生效的中断注入
    assert {r["id"] for r in result.recovered} == {"brk"}
    by_request = {c.request_id: c for c in result.comparisons}
    assert by_request[1].fault_status == "interrupted"
    assert by_request[2].fault_status == "interrupted"
    # 停止后的请求不再执行，与对照组对比为 not_executed
    assert by_request[3].fault_status == "not_executed"
    assert by_request[4].fault_status == "not_executed"
    kinds = [e["kind"] for e in result.event_log]
    assert "stop" in kinds
    assert kinds.count("request_skipped") == 2


def test_cancel_same_tick_as_request_runs_clean():
    # 一致规则：取消与请求同刻时先撤销故障，请求按无故障完成
    scenario = Scenario(
        name="same-tick", seed=11,
        requests=[RequestSpec(at_ms=100), RequestSpec(at_ms=200)],
        injections=[InjectionSpec("lat", "latency", "billing", 0, None,
                                  {"delay_ms": 40})],
        cancel_at_ms=200,
    )
    result = ExperimentRunner().run(scenario)
    assert result.cancelled is True
    by_request = {c.request_id: c for c in result.comparisons}
    assert by_request[0].latency_delta_ns == 40_000_000
    assert by_request[1].latency_delta_ns == 0
    assert by_request[1].status_changed is False


def test_event_log_replay_is_deterministic(tmp_path: Path):
    scenario = _dependency_scenario()
    runner = ExperimentRunner()
    first = runner.run(scenario).to_dict()
    second = runner.run(scenario).to_dict()
    assert first["event_log"] == second["event_log"]
    assert [e["kind"] for e in first["event_log"]] == [
        "request", "inject_start", "request", "inject_start",
        "request", "inject_end", "inject_end", "request",
    ]
    report = tmp_path / "dep.json"
    save_run(runner.run(scenario), scenario, report)
    assert cli_main(["replay", str(report)]) == 0
    # 篡改事件日志后重放必须失败
    saved = load_run(report)
    saved["result"]["event_log"][1]["at_ms"] += 1
    report.write_text(__import__("json").dumps(saved, ensure_ascii=False),
                      encoding="utf-8")
    assert cli_main(["replay", str(report)]) == 1


def test_pending_injection_skipped_when_window_elapses():
    scenario = Scenario(
        name="skip", seed=2,
        requests=[RequestSpec(at_ms=10)],
        injections=[
            InjectionSpec("tmo", "timeout", "billing", 100, 200,
                          {"threshold_ms": 10},
                          start_when={"kind": "failures_at_least", "count": 5}),
        ],
    )
    result = ExperimentRunner().run(scenario)
    kinds = [e["kind"] for e in result.event_log]
    assert "inject_skipped" in kinds
    assert "inject_start" not in kinds
    assert all(not c.status_changed for c in result.comparisons)
