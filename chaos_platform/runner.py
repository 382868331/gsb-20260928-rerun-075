"""实验运行控制。

职责：
* 按场景时间轴驱动注入的 start/end/restore；
* 对照组与故障组使用同一输入序列、同一种子，逐请求比较真实结果；
* 实验取消时撤销所有仍生效的故障；
* 故障类型完全来自注册表，新增故障类型无需修改本模块。
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from itertools import count
from typing import Dict, List, Optional

from .clock import VirtualClock
from .faults.base import FaultRegistry
from .manager import FaultManager
from .recorder import GroupRun, ObservationLog, RequestDiff, compare
from .scenario import Scenario
from .services import ServiceChain


@dataclass(order=True)
class _Event:
    time: float
    order: int
    kind: str = field(compare=False)
    payload: dict = field(compare=False, default_factory=dict)


@dataclass
class ExperimentResult:
    scenario_name: str
    seed: int
    cancelled: bool
    finished_at_ms: float
    control: GroupRun
    fault_group: GroupRun
    diffs: List[RequestDiff]
    active_on_cancel: List[str] = field(default_factory=list)
    restored_after_cancel: List[str] = field(default_factory=list)

    @property
    def affected_requests(self) -> List[RequestDiff]:
        return [d for d in self.diffs if d.changed]

    def summary(self) -> Dict[str, object]:
        return {
            "scenario": self.scenario_name,
            "seed": self.seed,
            "cancelled": self.cancelled,
            "requests": self.control.total,
            "control_failures": self.control.failed,
            "fault_failures": self.fault_group.failed,
            "control_observed_failure_rate": self.control.observed_failure_rate(),
            "fault_observed_failure_rate": self.fault_group.observed_failure_rate(),
            "control_avg_latency_ms": self.control.avg_latency_ms,
            "fault_avg_latency_ms": self.fault_group.avg_latency_ms,
            "affected_requests": len(self.affected_requests),
        }


class ExperimentRunner:
    """确定性的实验运行器（虚拟时钟，无真实等待）。"""

    def __init__(
        self,
        scenario: Scenario,
        request_interval_ms: float = 10.0,
        start_offset_ms: float = 5.0,
        registry: Optional[FaultRegistry] = None,
    ) -> None:
        self.scenario = scenario
        self.request_interval_ms = request_interval_ms
        self.start_offset_ms = start_offset_ms
        self.registry = registry or FaultRegistry
        self._cancel_requested = False

    def cancel(self) -> None:
        """请求取消下一个事件边界上的实验。"""
        self._cancel_requested = True

    def run(self, cancel_after_ms: Optional[float] = None) -> ExperimentResult:
        scenario = self.scenario
        cancel_flagged = self._cancel_requested
        self._cancel_requested = False
        timeline = VirtualClock()
        manager = FaultManager()

        control_chain = ServiceChain(
            VirtualClock(), scenario.seed, scenario.topology, rng_namespace="traffic"
        )
        fault_chain = ServiceChain(
            VirtualClock(), scenario.seed, scenario.topology, rng_namespace="traffic"
        )
        fault_chain.set_fault_hook(manager.hook)

        control_log = ObservationLog()
        fault_log = ObservationLog()

        seq = count()
        events: List[_Event] = []

        def push(time: float, kind: str, **payload: object) -> None:
            heapq.heappush(events, _Event(time, next(seq), kind, payload))

        for schedule in scenario.injections:
            push(schedule.start_ms, "fault_start", schedule=schedule)
            push(schedule.end_ms, "fault_end", injection_id=schedule.injection_id)

        for index, request in enumerate(scenario.requests):
            at = self.start_offset_ms + index * self.request_interval_ms
            push(at, "request", request=request)

        cancelled = False
        active_on_cancel: List[str] = []
        restored: List[str] = []

        while events:
            event = heapq.heappop(events)
            timeline.run_until(event.time)

            cancel_due = cancel_flagged or (
                cancel_after_ms is not None and timeline.now >= cancel_after_ms
            )
            if not cancelled and cancel_due:
                cancelled = True
                active_on_cancel = manager.active_ids
                restored = [f.injection_id for f in manager.cancel_all()]
                fault_chain.clear_fault_hook()
                break

            if event.kind == "fault_start":
                fault = self.registry.create(
                    {
                        "injection_id": event.payload["schedule"].injection_id,
                        "type": event.payload["schedule"].type,
                        "service": event.payload["schedule"].service,
                        "params": event.payload["schedule"].params,
                    }
                )
                manager.start(fault)
            elif event.kind == "fault_end":
                injection_id = event.payload["injection_id"]
                if manager.is_active(injection_id):
                    manager.end(injection_id)
            elif event.kind == "request":
                request = event.payload["request"]
                # 请求按派发时刻执行：独立执行钟累加各跳延迟，
                # 生效故障集合由派发时刻决定，处理耗时不推进实验时间轴。
                control_chain.clock = VirtualClock(start=event.time)
                fault_chain.clock = VirtualClock(start=event.time)
                control_log.add(control_chain.send(request))
                fault_log.add(fault_chain.send(request))

        control = GroupRun("control", control_log)
        fault_group = GroupRun("fault", fault_log)
        return ExperimentResult(
            scenario_name=scenario.name,
            seed=scenario.seed,
            cancelled=cancelled,
            finished_at_ms=timeline.now,
            control=control,
            fault_group=fault_group,
            diffs=compare(control, fault_group),
            active_on_cancel=active_on_cancel,
            restored_after_cancel=restored,
        )
