"""项目自带的虚拟服务链。

服务链是一个 网关 -> 服务A -> 服务B -> 数据库 的调用链。
请求逐跳处理，每跳产生基础延迟（由带种子的随机数决定），
因此同一输入序列 + 同一种子在对照组与故障组下得到逐毫秒一致的真实请求结果。
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Optional

from .clock import VirtualClock

DEFAULT_TOPOLOGY: tuple[str, ...] = (
    "gateway",
    "service-a",
    "service-b",
    "database",
)


@dataclass(frozen=True)
class ServiceRequest:
    """一次真实请求（实验输入序列中的一项）。"""

    request_id: str
    path: str = "/api/query"
    payload_size: int = 1
    timeout: Optional[float] = None


@dataclass
class HopRecord:
    service: str
    base_latency: float
    injected_latency: float = 0.0
    aborted: bool = False
    error: Optional[str] = None

    @property
    def latency(self) -> float:
        return self.base_latency + self.injected_latency


@dataclass
class RequestResult:
    request_id: str
    started_at: float
    success: bool
    status: str
    hops: list[HopRecord] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def latency(self) -> float:
        return sum(hop.latency for hop in self.hops)

    @property
    def reached_service(self) -> Optional[str]:
        if not self.hops:
            return None
        return self.hops[-1].service


FaultHook = Callable[["ServiceChain", ServiceRequest, str, int, HopRecord], None]


class ServiceChain:
    """虚拟服务链。

    每一跳查询当前生效的故障：故障可以追加延迟或直接中断该跳。
    """

    def __init__(
        self,
        clock: VirtualClock,
        seed: int,
        topology: tuple[str, ...] = DEFAULT_TOPOLOGY,
        rng_namespace: str = "baseline",
    ) -> None:
        if not topology:
            raise ValueError("服务链不能为空")
        self.clock = clock
        self.topology = tuple(topology)
        self._seed = seed
        self._rng_namespace = rng_namespace
        self.fault_hook: Optional[FaultHook] = None

    def set_fault_hook(self, hook: FaultHook) -> None:
        self.fault_hook = hook

    def clear_fault_hook(self) -> None:
        self.fault_hook = None

    def _base_latency(self, service: str) -> float:
        # 按 请求+跳 派生随机数：即使某跳被中断、后续跳未执行，
        # 也不会改变其他请求/跳的基线抖动。
        rng = random.Random(
            f"{self._seed}:{self._rng_namespace}:{self._current_request}:{service}"
        )
        low, high = (4.0, 18.0) if service == "database" else (2.0, 12.0)
        return round(rng.uniform(low, high), 3)

    def send(self, request: ServiceRequest) -> RequestResult:
        self._current_request = request.request_id
        started_at = self.clock.now
        result = RequestResult(
            request_id=request.request_id,
            started_at=started_at,
            success=False,
            status="error",
        )
        for index, service in enumerate(self.topology):
            hop = HopRecord(service=service, base_latency=self._base_latency(service))
            result.hops.append(hop)
            if self.fault_hook is not None:
                self.fault_hook(self, request, service, index, hop)
            self.clock.sleep(hop.injected_latency)
            if hop.aborted:
                result.error = hop.error or f"fault abort at {service}"
                result.status = f"aborted@{service}"
                return result
            self.clock.sleep(hop.base_latency)
            if request.timeout is not None and self.clock.now - started_at > request.timeout:
                result.error = f"timeout after {service}"
                result.status = f"timeout@{service}"
                return result
        result.success = True
        result.status = "ok"
        return result


__all__ = [
    "DEFAULT_TOPOLOGY",
    "HopRecord",
    "RequestResult",
    "ServiceChain",
    "ServiceRequest",
]
