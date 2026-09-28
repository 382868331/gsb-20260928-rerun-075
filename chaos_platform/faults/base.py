"""故障注入的可插拔基类与注册表。

生命周期（由运行控制统一驱动，故障类型自身不感知时间轴）：

start -> （多次 apply_hop） -> end -> restore

* start：故障生效，可占用资源；
* end：注入窗口结束，故障不再影响新请求；
* restore：撤销残留副作用，使系统恢复到健康状态。

实验取消时，运行控制会对所有仍处于生效状态的注入依次调用 end/restore，
因此取消后不会留下任何故障。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type


@dataclass
class FaultContext:
    """单次请求、单跳处理时传给故障的上下文。"""

    injection_id: str
    service: str
    hop_index: int
    request_id: str
    params: Dict[str, Any] = field(default_factory=dict)


class Fault:
    """所有故障类型的基类。

    子类通过类属性 type_name 声明注册名，并按需覆盖生命周期钩子。
    target_service 为 None 时作用于整条服务链的每一跳。
    """

    type_name: str = ""

    def __init__(
        self,
        injection_id: str,
        target_service: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.type_name:
            raise ValueError(f"{type(self).__name__} 缺少 type_name")
        self.injection_id = injection_id
        self.target_service = target_service
        self.params: Dict[str, Any] = dict(params or {})
        self.started = False
        self.ended = False
        self.restored = False

    def matches(self, service: str) -> bool:
        return self.target_service is None or self.target_service == service

    def on_start(self) -> None:
        """故障开始生效。"""

    def apply_hop(self, ctx: FaultContext, hop) -> None:
        """对匹配的一跳施加影响（追加延迟 / 置中断标志等）。"""

    def on_end(self) -> None:
        """注入窗口结束。"""

    def on_restore(self) -> None:
        """撤销残留副作用，恢复健康状态。"""

    def to_config(self) -> Dict[str, Any]:
        return {
            "injection_id": self.injection_id,
            "type": self.type_name,
            "service": self.target_service,
            "params": dict(self.params),
        }


class FaultRegistry:
    """故障类型注册表：运行控制只依赖注册表，新增类型无需修改运行控制。"""

    _types: Dict[str, Type[Fault]] = {}

    @classmethod
    def register(cls, fault_cls: Type[Fault]) -> Type[Fault]:
        name = fault_cls.type_name
        if not name:
            raise ValueError("故障类型必须声明非空 type_name")
        cls._types[name] = fault_cls
        return fault_cls

    @classmethod
    def get(cls, name: str) -> Type[Fault]:
        try:
            return cls._types[name]
        except KeyError:
            raise KeyError(f"未注册的故障类型: {name}") from None

    @classmethod
    def names(cls) -> list[str]:
        return sorted(cls._types)

    @classmethod
    def create(cls, config: Dict[str, Any]) -> Fault:
        fault_cls = cls.get(str(config["type"]))
        return fault_cls(
            injection_id=str(config["injection_id"]),
            target_service=config.get("service"),
            params=config.get("params") or {},
        )


def fault(cls: Type[Fault]) -> Type[Fault]:
    """类装饰器：声明一个故障类型并注册。"""
    return FaultRegistry.register(cls)
