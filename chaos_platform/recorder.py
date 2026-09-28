"""观察记录：保存真实请求结果并汇总对照组/故障组差异。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .services import RequestResult


@dataclass
class Observation:
    observed_at: float
    request_id: str
    success: bool
    status: str
    latency_ms: float
    reached_service: Optional[str]
    error: Optional[str]

    @classmethod
    def from_result(cls, result: RequestResult) -> "Observation":
        return cls(
            observed_at=result.started_at,
            request_id=result.request_id,
            success=result.success,
            status=result.status,
            latency_ms=round(result.latency, 3),
            reached_service=result.reached_service,
            error=result.error,
        )


@dataclass
class ObservationLog:
    observations: List[Observation] = field(default_factory=list)

    def add(self, result: RequestResult) -> Observation:
        obs = Observation.from_result(result)
        self.observations.append(obs)
        return obs

    def by_request(self) -> Dict[str, Observation]:
        return {obs.request_id: obs for obs in self.observations}


@dataclass
class RequestDiff:
    request_id: str
    changed: bool
    control_status: str
    fault_status: str
    latency_delta_ms: float
    control_error: Optional[str]
    fault_error: Optional[str]


@dataclass
class GroupRun:
    name: str
    log: ObservationLog

    @property
    def total(self) -> int:
        return len(self.log.observations)

    @property
    def succeeded(self) -> int:
        return sum(1 for o in self.log.observations if o.success)

    @property
    def failed(self) -> int:
        return self.total - self.succeeded

    @property
    def avg_latency_ms(self) -> float:
        if not self.log.observations:
            return 0.0
        return round(
        sum(o.latency_ms for o in self.log.observations) / len(self.log.observations), 3
    )

    def observed_failure_rate(self) -> float:
        """由真实请求结果统计出的失败率（而非预设的固定故障率）。"""
        if self.total == 0:
            return 0.0
        return round(self.failed / self.total, 4)


def compare(control: GroupRun, fault_group: GroupRun) -> List[RequestDiff]:
    """按输入序列逐请求比较两组的真实结果。"""
    control_map = control.log.by_request()
    fault_map = fault_group.log.by_request()
    diffs: List[RequestDiff] = []
    for request_id in control_map:
        base = control_map[request_id]
        injured = fault_map.get(request_id)
        if injured is None:
            diffs.append(
                RequestDiff(
                    request_id=request_id,
                    changed=True,
                    control_status=base.status,
                    fault_status="<missing>",
                    latency_delta_ms=-base.latency_ms,
                    control_error=base.error,
                    fault_error="请求未发出（实验可能被取消）",
                )
            )
            continue
        delta = round(injured.latency_ms - base.latency_ms, 3)
        changed = (
            base.success != injured.success
            or base.status != injured.status
            or delta != 0.0
            or base.error != injured.error
        )
        diffs.append(
            RequestDiff(
                request_id=request_id,
                changed=changed,
                control_status=base.status,
                fault_status=injured.status,
                latency_delta_ms=delta,
                control_error=base.error,
                fault_error=injured.error,
            )
        )
    return diffs
