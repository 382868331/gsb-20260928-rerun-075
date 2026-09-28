"""延迟故障：在匹配跳上追加固定延迟（毫秒）。"""

from __future__ import annotations

from .base import Fault, FaultContext, fault


@fault
class DelayFault(Fault):
    type_name = "delay"

    def apply_hop(self, ctx: FaultContext, hop) -> None:
        delay_ms = float(self.params.get("delay_ms", 0.0))
        hop.injected_latency += delay_ms
