"""中断故障：在匹配跳直接中断请求，返回错误。"""

from __future__ import annotations

from .base import Fault, fault


@fault
class AbortFault(Fault):
    type_name = "abort"

    def apply_hop(self, ctx: FaultContext, hop) -> None:
        hop.aborted = True
        hop.error = str(self.params.get("error") or f"injected abort at {hop.service}")
