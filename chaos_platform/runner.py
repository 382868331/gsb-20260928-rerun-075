"""运行控制：时间线驱动、开始/结束/恢复、取消撤销、双组对照。"""

from dataclasses import dataclass, field

from .clock import VirtualClock
from .faults import FaultManager, fault_registry
from .scenario import Scenario
from .service_chain import CallResult, ServiceChain
from .adaptive import AdaptiveEngine


@dataclass
class RunConfig:
    horizon_ms: int | None = None  # 不指定时由场景末尾决定


@dataclass
class Observation:
    request_id: int
    group: str  # control / fault
    scheduled_at_ms: int
    result: dict

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "group": self.group,
            "scheduled_at_ms": self.scheduled_at_ms,
            "result": self.result,
        }


@dataclass
class Comparison:
    request_id: int
    at_ms: int
    status_changed: bool
    control_status: str
    fault_status: str
    latency_delta_ns: int
    failed_node: str | None

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "at_ms": self.at_ms,
            "status_changed": self.status_changed,
            "control_status": self.control_status,
            "fault_status": self.fault_status,
            "latency_delta_ns": self.latency_delta_ns,
            "failed_node": self.failed_node,
        }


@dataclass
class RunResult:
    scenario_name: str
    seed: int
    cancelled: bool
    cancel_at_ms: int | None
    recovered: list[dict]
    observations: list[Observation] = field(default_factory=list)
    comparisons: list[Comparison] = field(default_factory=list)
    fault_events: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "scenario_name": self.scenario_name,
            "seed": self.seed,
            "cancelled": self.cancelled,
            "cancel_at_ms": self.cancel_at_ms,
            "recovered": self.recovered,
            "observations": [o.to_dict() for o in self.observations],
            "comparisons": [c.to_dict() for c in self.comparisons],
            "fault_events": self.fault_events,
        }


@dataclass
class _TimelineEvent:
    at_ms: int
    kind: str  # inject_start / inject_end / cancel / request / finish
    payload: dict = field(default_factory=dict)


class ExperimentRunner:
    """同一场景跑两遍：无故障对照组与故障组，输入序列完全相同。"""

    def __init__(self, chain: ServiceChain | None = None,
                 manager_factory=lambda: FaultManager(fault_registry)) -> None:
        self.chain = chain or ServiceChain()
        self.manager_factory = manager_factory
        self.adaptive_engine = AdaptiveEngine(self.chain, manager_factory)

    def _build_timeline(self, scenario: Scenario, config: RunConfig,
                        include_faults: bool) -> list[_TimelineEvent]:
        events: list[_TimelineEvent] = [
            _TimelineEvent(r.at_ms, "request",
                           {"request_id": idx, "payload": r.payload})
            for idx, r in enumerate(scenario.requests)
        ]
        if include_faults:
            for injection in scenario.injections:
                events.append(_TimelineEvent(
                    injection.start_ms, "inject_start", {"spec": injection}))
                if injection.end_ms is not None:
                    events.append(_TimelineEvent(
                        injection.end_ms, "inject_end", {"id": injection.id}))
            if scenario.cancel_at_ms is not None:
                events.append(_TimelineEvent(
                    scenario.cancel_at_ms, "cancel"))
        horizon = config.horizon_ms
        if horizon is None:
            candidate = [r.at_ms for r in scenario.requests]
            if include_faults:
                candidate += [i.start_ms for i in scenario.injections]
                candidate += [i.end_ms for i in scenario.injections
                              if i.end_ms is not None]
                if scenario.cancel_at_ms is not None:
                    candidate.append(scenario.cancel_at_ms)
            horizon = max(candidate, default=0)
        events.append(_TimelineEvent(horizon, "finish"))
        # 同刻顺序：开始 -> 结束/取消 -> 请求 -> finish，保证边界时刻语义明确。
        order = {"inject_start": 0, "inject_end": 1, "cancel": 1,
                 "request": 2, "finish": 3}
        return sorted(events, key=lambda e: (e.at_ms, order[e.kind]))

    def _run_group(self, scenario: Scenario, config: RunConfig,
                   group: str, apply_faults: bool):
        clock = VirtualClock()
        manager = self.manager_factory()
        observations: list[Observation] = []
        recovered: list[dict] = []
        cancelled = False
        for event in self._build_timeline(scenario, config, apply_faults):
            clock.advance_to(VirtualClock.ms_to_ns(event.at_ms))
            if event.kind == "inject_start":
                manager.start(event.payload["spec"], event.at_ms)
            elif event.kind == "inject_end":
                manager.end(event.payload["id"], event.at_ms)
            elif event.kind == "cancel":
                cancelled = True
                handles = manager.recover_all(event.at_ms, reason="experiment_cancelled")
                recovered.extend(
                    {"id": h.injection_id, "type": h.type_name,
                     "target": h.target, "at_ms": event.at_ms} for h in handles
                )
            elif event.kind == "request":
                result: CallResult = self.chain.call(
                    request_id=event.payload["request_id"],
                    payload=event.payload["payload"],
                    seed=scenario.seed,
                    manager=manager if apply_faults else None,
                )
                observations.append(Observation(
                    request_id=event.payload["request_id"],
                    group=group,
                    scheduled_at_ms=event.at_ms,
                    result=result.to_dict(),
                ))
            elif event.kind == "finish" and apply_faults:
                # 正常结束也要保证现场归零（未显式 end 的注入）。
                handles = manager.recover_all(event.at_ms, reason="experiment_finished")
                recovered.extend(
                    {"id": h.injection_id, "type": h.type_name,
                     "target": h.target, "at_ms": event.at_ms} for h in handles
                )
        if not manager.is_empty():
            raise RuntimeError("演练结束后仍有故障未恢复")
        return observations, manager.history, recovered, cancelled

    def run(self, scenario: Scenario, config: RunConfig | None = None) -> RunResult:
        config = config or RunConfig()
        control_obs, _, _, _ = self._run_group(scenario, config, "control", False)
        fault_obs, fault_events, recovered, cancelled = self._run_group(
            scenario, config, "fault", True)
        comparisons = self._compare(control_obs, fault_obs)
        return RunResult(
            scenario_name=scenario.name,
            seed=scenario.seed,
            cancelled=cancelled,
            cancel_at_ms=scenario.cancel_at_ms,
            recovered=recovered,
            observations=control_obs + fault_obs,
            comparisons=comparisons,
            fault_events=fault_events,
        )

    def run_adaptive_branch(
        self,
        scenario: Scenario,
        thresholds: dict,
        branch_id: str = "root",
        config: RunConfig | None = None,
        approvals: list[dict] | None = None,
    ) -> dict:
        config = config or RunConfig()
        return self.adaptive_engine.run_branch(
            scenario=scenario,
            thresholds=thresholds,
            branch_id=branch_id,
            horizon_ms=config.horizon_ms,
            approvals=approvals,
        )

    def run_adaptive_experiment(
        self,
        scenario: Scenario,
        root_thresholds: dict,
        alternate_thresholds: dict | None = None,
        config: RunConfig | None = None,
        approvals: list[dict] | None = None,
        alternate_approvals: list[dict] | None = None,
    ) -> dict:
        branches = [self.run_adaptive_branch(scenario, root_thresholds, "root",
                                             config, approvals)]
        if alternate_thresholds is not None:
            branches.append(self.run_adaptive_branch(
                scenario, alternate_thresholds, "alternate", config,
                alternate_approvals))
        return {
            "scenario_name": scenario.name,
            "seed": scenario.seed,
            "branches": branches,
        }

    @staticmethod
    def _compare(control: list[Observation], fault: list[Observation]) -> list[Comparison]:
        by_id = {o.request_id: o for o in control}
        comparisons: list[Comparison] = []
        for obs in fault:
            baseline = by_id[obs.request_id]
            base_result = baseline.result
            fault_result = obs.result
            comparisons.append(Comparison(
                request_id=obs.request_id,
                at_ms=obs.scheduled_at_ms,
                status_changed=base_result["status"] != fault_result["status"],
                control_status=base_result["status"],
                fault_status=fault_result["status"],
                latency_delta_ns=fault_result["total_latency_ns"]
                - base_result["total_latency_ns"],
                failed_node=fault_result.get("failed_node"),
            ))
        return comparisons
