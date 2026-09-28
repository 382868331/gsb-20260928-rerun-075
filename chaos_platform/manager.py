"""故障注入管理器：维护当前生效的注入并驱动其生命周期。"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from .faults.base import Fault, FaultContext


class InjectionStillActive(RuntimeError):
    pass


class FaultManager:
    """按 injection_id 独立管理每一项注入。

    重叠注入是不同的 Fault 实例：结束/恢复其中一项只操作该实例，
    绝不会误撤同一时间窗内仍然生效的另一项故障。
    """

    def __init__(self) -> None:
        self._active: Dict[str, Fault] = {}
        self.history: List[str] = []

    @property
    def active_ids(self) -> list[str]:
        return sorted(self._active)

    def is_active(self, injection_id: str) -> bool:
        return injection_id in self._active

    def start(self, fault: Fault) -> None:
        if fault.injection_id in self._active:
            raise InjectionStillActive(f"注入已生效: {fault.injection_id}")
        fault.on_start()
        fault.started = True
        self._active[fault.injection_id] = fault
        self.history.append(f"start:{fault.injection_id}")

    def end(self, injection_id: str) -> Fault:
        fault = self._pop(injection_id)
        fault.on_end()
        fault.ended = True
        fault.on_restore()
        fault.restored = True
        self.history.append(f"end:{injection_id}")
        return fault

    def cancel_all(self) -> list[Fault]:
        """撤销所有仍生效的故障（实验取消路径）。"""
        cancelled: list[Fault] = []
        for injection_id in sorted(self._active):
            fault = self._pop(injection_id)
            fault.on_end()
            fault.ended = True
            fault.on_restore()
            fault.restored = True
            cancelled.append(fault)
            self.history.append(f"cancel:{injection_id}")
        return cancelled

    def _pop(self, injection_id: str) -> Fault:
        try:
            return self._active.pop(injection_id)
        except KeyError:
            raise KeyError(f"注入不存在或已结束: {injection_id}") from None

    def active_faults(self, service: Optional[str] = None) -> Iterable[Fault]:
        faults = list(self._active.values())
        if service is None:
            return faults
        return [f for f in faults if f.matches(service)]

    def hook(self, chain, request, service: str, hop_index: int, hop) -> None:
        """挂到 ServiceChain 上的故障访问点。"""
        for fault in list(self._active.values()):
            if not fault.matches(service):
                continue
            ctx = FaultContext(
                injection_id=fault.injection_id,
                service=service,
                hop_index=hop_index,
                request_id=request.request_id,
                params=fault.params,
            )
            fault.apply_hop(ctx, hop)
