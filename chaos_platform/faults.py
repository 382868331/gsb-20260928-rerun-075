"""故障注入抽象与内置类型。

新增故障类型时只需实现 FaultType 并注册到 fault_registry，
运行控制（ExperimentRunner / FaultManager）无需修改。
"""

from dataclasses import dataclass, field
from typing import Callable, Protocol

from .scenario import InjectionSpec


@dataclass
class EffectHandle:
    """一次注入生效后产生的、可独立撤销的效果句柄。"""

    injection_id: str
    type_name: str
    target: str
    aspect: str  # 作用维度，如 latency / interruption
    value_ns: int = 0
    detail: dict = field(default_factory=dict)


class FaultType(Protocol):
    type_name: str

    def create_handle(self, spec: InjectionSpec) -> EffectHandle: ...


@dataclass(frozen=True)
class LatencyFault:
    type_name: str = "latency"

    def create_handle(self, spec: InjectionSpec) -> EffectHandle:
        delay_ms = spec.params.get("delay_ms")
        if not isinstance(delay_ms, (int, float)) or delay_ms < 0:
            raise ValueError(f"注入 {spec.id} 的 delay_ms 必须是非负数")
        return EffectHandle(
            injection_id=spec.id,
            type_name=self.type_name,
            target=spec.target,
            aspect="latency",
            value_ns=int(delay_ms * 1_000_000),
            detail={"delay_ms": delay_ms},
        )


@dataclass(frozen=True)
class InterruptionFault:
    type_name: str = "interruption"

    def create_handle(self, spec: InjectionSpec) -> EffectHandle:
        return EffectHandle(
            injection_id=spec.id,
            type_name=self.type_name,
            target=spec.target,
            aspect="interruption",
        )


class FaultRegistry:
    def __init__(self) -> None:
        self._types: dict[str, FaultType] = {}

    def register(self, fault_type: FaultType) -> None:
        name = fault_type.type_name
        if name in self._types:
            raise ValueError(f"故障类型 {name} 已注册")
        self._types[name] = fault_type

    def get(self, name: str) -> FaultType:
        try:
            return self._types[name]
        except KeyError:
            raise KeyError(f"未注册的故障类型: {name}") from None

    def names(self) -> list[str]:
        return sorted(self._types)


fault_registry = FaultRegistry()
fault_registry.register(LatencyFault())
fault_registry.register(InterruptionFault())


@dataclass
class ActiveEffect:
    handle: EffectHandle
    started_ms: int


class FaultManager:
    """管理重叠注入：按维度+目标堆叠效果，按注入 ID 精确撤销。"""

    def __init__(self, registry: FaultRegistry | None = None) -> None:
        self.registry = registry or fault_registry
        self._effects: dict[tuple[str, str], list[ActiveEffect]] = {}
        self.history: list[dict] = []

    def _key(self, handle: EffectHandle) -> tuple[str, str]:
        return (handle.aspect, handle.target)

    def start(self, spec: InjectionSpec, now_ms: int) -> EffectHandle:
        handle = self.registry.get(spec.type_name).create_handle(spec)
        self._effects.setdefault(self._key(handle), []).append(
            ActiveEffect(handle, now_ms)
        )
        self.history.append(
            {"event": "inject_start", "id": spec.id, "type": spec.type_name,
             "target": spec.target, "at_ms": now_ms}
        )
        return handle

    def end(self, injection_id: str, now_ms: int) -> list[EffectHandle]:
        removed: list[EffectHandle] = []
        for effects in self._effects.values():
            kept: list[ActiveEffect] = []
            for active in effects:
                if active.handle.injection_id == injection_id:
                    removed.append(active.handle)
                else:
                    kept.append(active)
            effects[:] = kept
        for handle in removed:
            self.history.append(
                {"event": "inject_end", "id": injection_id,
                 "type": handle.type_name, "target": handle.target, "at_ms": now_ms}
            )
        return removed

    def recover_all(self, now_ms: int, reason: str) -> list[EffectHandle]:
        active = [
            active.handle
            for effects in self._effects.values()
            for active in effects
        ]
        # 按开始时间逆序撤销，模拟现场恢复顺序。
        ordered = sorted(active, key=lambda h: (self._started_at(h), h.injection_id),
                         reverse=True)
        for handle in ordered:
            self.end(handle.injection_id, now_ms)
            self.history.append(
                {"event": "recover", "id": handle.injection_id,
                 "type": handle.type_name, "target": handle.target,
                 "at_ms": now_ms, "reason": reason}
            )
        return ordered

    def _started_at(self, handle: EffectHandle) -> int:
        for active in self._effects.get(self._key(handle), []):
            if active.handle is handle:
                return active.started_ms
        return -1

    def active_ids(self) -> list[str]:
        return sorted(
            {active.handle.injection_id
             for effects in self._effects.values()
             for active in effects}
        )

    def conflicts(self, spec: InjectionSpec) -> list[str]:
        """与 spec 作用于同一 (维度, 目标) 的仍在生效的注入 ID。"""
        handle = self.registry.get(spec.type_name).create_handle(spec)
        key = self._key(handle)
        return sorted(
            {active.handle.injection_id
             for active in self._effects.get(key, [])
             if active.handle.injection_id != spec.id}
        )

    def added_latency_ns(self, target: str) -> int:
        return sum(
            active.handle.value_ns
            for active in self._effects.get(("latency", target), [])
        )

    def interruption_active(self, target: str) -> bool:
        return bool(self._effects.get(("interruption", target)))

    def is_empty(self) -> bool:
        return not self.active_ids()
