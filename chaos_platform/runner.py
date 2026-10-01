"""运行控制：时间线驱动、依赖型注入、提前停止、双组对照与逐事件日志。

同刻排序规则（边界语义一致）：
    inject_start(0) -> inject_end/cancel(1) -> request(2) -> finish(3)
即取消/结束与请求同刻时，先撤销故障再执行请求，该请求按无故障完成；
提前停止在每个请求处理完之后评估，因此停止时刻同刻的请求照常完成，
停止之后时刻的请求一律记为 not_executed（在途请求不存在：请求是
同步模型，同刻内立即完成或重试完毕）。
"""

from dataclasses import dataclass, field

from .clock import VirtualClock
from .faults import FaultManager, fault_registry
from .scenario import Scenario
from .service_chain import CallResult, ServiceChain


@dataclass
class RunConfig:
    horizon_ms: int | None = None  # 不指定时由场景末尾决定


@dataclass
class Observation:
    request_id: int
    group: str  # control / fault
    scheduled_at_ms: int
    result: dict
    attempts: list[dict] = field(default_factory=list)  # 含每次重试

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "group": self.group,
            "scheduled_at_ms": self.scheduled_at_ms,
            "result": self.result,
            "attempts": self.attempts,
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
    attributed_to: list[str] = field(default_factory=list)  # 造成影响的注入 ID

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "at_ms": self.at_ms,
            "status_changed": self.status_changed,
            "control_status": self.control_status,
            "fault_status": self.fault_status,
            "latency_delta_ns": self.latency_delta_ns,
            "failed_node": self.failed_node,
            "attributed_to": self.attributed_to,
        }


@dataclass
class RunResult:
    scenario_name: str
    seed: int
    cancelled: bool
    cancel_at_ms: int | None
    stopped: bool
    stopped_at_ms: int | None
    stop_reason: str | None
    recovered: list[dict]
    observations: list[Observation] = field(default_factory=list)
    comparisons: list[Comparison] = field(default_factory=list)
    fault_events: list[dict] = field(default_factory=list)
    event_log: list[dict] = field(default_factory=list)  # 逐事件日志，供重放比对

    def to_dict(self) -> dict:
        return {
            "scenario_name": self.scenario_name,
            "seed": self.seed,
            "cancelled": self.cancelled,
            "cancel_at_ms": self.cancel_at_ms,
            "stopped": self.stopped,
            "stopped_at_ms": self.stopped_at_ms,
            "stop_reason": self.stop_reason,
            "recovered": self.recovered,
            "observations": [o.to_dict() for o in self.observations],
            "comparisons": [c.to_dict() for c in self.comparisons],
            "fault_events": self.fault_events,
            "event_log": self.event_log,
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

    def _build_timeline(self, scenario: Scenario, config: RunConfig,
                        include_faults: bool) -> list[_TimelineEvent]:
        events: list[_TimelineEvent] = [
            _TimelineEvent(r.at_ms, "request",
                           {"request_id": idx, "payload": r.payload,
                            "retries": r.retries})
            for idx, r in enumerate(scenario.requests)
        ]
        if include_faults:
            for injection in scenario.injections:
                if not injection.conditional:
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
        event_log: list[dict] = []
        cancelled = False
        stopped = False
        stopped_at_ms: int | None = None
        stop_reason: str | None = None
        skipped_requests: list[int] = []
        pending = [inj for inj in scenario.injections
                   if apply_faults and inj.conditional]

        def log(kind: str, at_ms: int, **fields) -> None:
            event_log.append({"seq": len(event_log), "at_ms": at_ms,
                              "kind": kind, **fields})

        def condition_met(cond: dict) -> bool:
            kind = cond["kind"]
            if kind == "latency_at_least":
                limit_ns = int(cond["ms"] * 1_000_000)
                return any(
                    hop["node"] == cond["target"] and hop["latency_ns"] >= limit_ns
                    for obs in observations
                    for attempt in obs.attempts
                    for hop in attempt["hops"]
                )
            if kind == "failures_at_least":
                failed = sum(1 for o in observations
                             if o.result["status"] != "ok")
                return failed >= int(cond["count"])
            raise ValueError(f"未知观察条件: {kind}")

        def try_fire_pending(now_ms: int) -> None:
            for injection in list(pending):
                if now_ms < injection.start_ms:
                    continue
                if (injection.end_ms is not None
                        and now_ms > injection.end_ms):
                    pending.remove(injection)
                    log("inject_skipped", now_ms, id=injection.id,
                        reason="window_elapsed_before_conditions_met")
                    continue
                if not all(dep in manager.active_ids()
                           for dep in injection.depends_on):
                    continue
                if injection.start_when is not None \
                        and not condition_met(injection.start_when):
                    continue
                manager.start(injection, now_ms)
                pending.remove(injection)
                log("inject_start", now_ms, id=injection.id,
                    type=injection.type_name, target=injection.target,
                    conditional=True)

        def stop_triggered() -> dict | None:
            for cond in scenario.stop_when:
                kind = cond["kind"]
                if kind == "consecutive_failures":
                    count = int(cond["count"])
                    recent = observations[-count:]
                    if len(recent) == count and all(
                            o.result["status"] != "ok" for o in recent):
                        return cond
                elif kind == "failure_rate_at_least":
                    if len(observations) >= int(cond.get("min_requests", 1)):
                        failed = sum(1 for o in observations
                                     if o.result["status"] != "ok")
                        if failed / len(observations) >= float(cond["rate"]):
                            return cond
                else:
                    raise ValueError(f"未知停止条件: {kind}")
            return None

        for event in self._build_timeline(scenario, config, apply_faults):
            clock.advance_to(VirtualClock.ms_to_ns(event.at_ms))
            if event.kind == "inject_start":
                manager.start(event.payload["spec"], event.at_ms)
                log("inject_start", event.at_ms, id=event.payload["spec"].id,
                    type=event.payload["spec"].type_name,
                    target=event.payload["spec"].target, conditional=False)
            elif event.kind == "inject_end":
                removed = manager.end(event.payload["id"], event.at_ms)
                if removed:
                    log("inject_end", event.at_ms, id=event.payload["id"])
                elif any(p.id == event.payload["id"] for p in pending):
                    pending[:] = [p for p in pending
                                  if p.id != event.payload["id"]]
                    log("inject_skipped", event.at_ms, id=event.payload["id"],
                        reason="window_elapsed_before_conditions_met")
            elif event.kind == "cancel":
                cancelled = True
                pending.clear()  # 取消后待定注入不再生效
                handles = manager.recover_all(event.at_ms, reason="experiment_cancelled")
                recovered.extend(
                    {"id": h.injection_id, "type": h.type_name,
                     "target": h.target, "at_ms": event.at_ms} for h in handles
                )
                log("cancel", event.at_ms,
                    recovered=[h.injection_id for h in handles])
            elif event.kind == "request":
                request_id = event.payload["request_id"]
                if stopped:
                    # 提前停止后不再发起新请求；同刻请求已在停止评估前完成。
                    skipped_requests.append(request_id)
                    log("request_skipped", event.at_ms, request_id=request_id)
                    continue
                attempts: list[dict] = []
                final: CallResult | None = None
                for _ in range(event.payload["retries"] + 1):
                    final = self.chain.call(
                        request_id=request_id,
                        payload=event.payload["payload"],
                        seed=scenario.seed,
                        manager=manager if apply_faults else None,
                    )
                    attempts.append(final.to_dict())
                    if final.status == "ok":
                        break
                observations.append(Observation(
                    request_id=request_id,
                    group=group,
                    scheduled_at_ms=event.at_ms,
                    result=final.to_dict(),
                    attempts=attempts,
                ))
                log("request", event.at_ms, request_id=request_id,
                    attempts=len(attempts), status=final.status,
                    failed_by=final.failed_by)
                if apply_faults and not cancelled:
                    cond = stop_triggered()
                    if cond is not None:
                        stopped = True
                        stopped_at_ms = event.at_ms
                        stop_reason = (
                            f"{cond['kind']}: "
                            + ", ".join(f"{k}={v}" for k, v in cond.items()
                                        if k != "kind")
                        )
                        handles = manager.recover_all(event.at_ms,
                                                      reason="early_stop")
                        pending.clear()  # 提前停止后待定注入不再生效
                        recovered.extend(
                            {"id": h.injection_id, "type": h.type_name,
                             "target": h.target, "at_ms": event.at_ms}
                            for h in handles
                        )
                        log("stop", event.at_ms, reason=stop_reason,
                            recovered=[h.injection_id for h in handles])
            elif event.kind == "finish" and apply_faults:
                # 正常结束也要保证现场归零（未显式 end 的注入）。
                handles = manager.recover_all(event.at_ms, reason="experiment_finished")
                recovered.extend(
                    {"id": h.injection_id, "type": h.type_name,
                     "target": h.target, "at_ms": event.at_ms} for h in handles
                )
            if apply_faults and not stopped:
                try_fire_pending(event.at_ms)
        if not manager.is_empty():
            raise RuntimeError("演练结束后仍有故障未恢复")
        meta = {
            "cancelled": cancelled,
            "stopped": stopped,
            "stopped_at_ms": stopped_at_ms,
            "stop_reason": stop_reason,
            "skipped_requests": skipped_requests,
        }
        return observations, manager.history, recovered, event_log, meta

    def run(self, scenario: Scenario, config: RunConfig | None = None) -> RunResult:
        config = config or RunConfig()
        control_obs, _, _, _, _ = self._run_group(scenario, config, "control", False)
        fault_obs, fault_events, recovered, event_log, meta = self._run_group(
            scenario, config, "fault", True)
        comparisons = self._compare(control_obs, fault_obs,
                                    meta["skipped_requests"])
        return RunResult(
            scenario_name=scenario.name,
            seed=scenario.seed,
            cancelled=meta["cancelled"],
            cancel_at_ms=scenario.cancel_at_ms,
            stopped=meta["stopped"],
            stopped_at_ms=meta["stopped_at_ms"],
            stop_reason=meta["stop_reason"],
            recovered=recovered,
            observations=control_obs + fault_obs,
            comparisons=comparisons,
            fault_events=fault_events,
            event_log=event_log,
        )

    @staticmethod
    def _compare(control: list[Observation], fault: list[Observation],
                 skipped: list[int]) -> list[Comparison]:
        by_id = {o.request_id: o for o in control}
        fault_by_id = {o.request_id: o for o in fault}
        comparisons: list[Comparison] = []
        for obs in control:
            fault_obs = fault_by_id.get(obs.request_id)
            if fault_obs is None:
                # 提前停止后被丢弃的请求：对照组正常，故障组未执行。
                comparisons.append(Comparison(
                    request_id=obs.request_id,
                    at_ms=obs.scheduled_at_ms,
                    status_changed=True,
                    control_status=obs.result["status"],
                    fault_status="not_executed",
                    latency_delta_ns=0,
                    failed_node=None,
                    attributed_to=[],
                ))
                continue
            baseline = by_id[fault_obs.request_id]
            base_result = baseline.result
            fault_result = fault_obs.result
            attributed = sorted({
                source
                for attempt in fault_obs.attempts
                for source in (*attempt["contributions"].keys(),
                               *attempt["failed_by"])
            })
            comparisons.append(Comparison(
                request_id=fault_obs.request_id,
                at_ms=fault_obs.scheduled_at_ms,
                status_changed=base_result["status"] != fault_result["status"],
                control_status=base_result["status"],
                fault_status=fault_result["status"],
                latency_delta_ns=fault_result["total_latency_ns"]
                - base_result["total_latency_ns"],
                failed_node=fault_result.get("failed_node"),
                attributed_to=attributed,
            ))
        return comparisons
