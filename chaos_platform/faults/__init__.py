"""故障类型集合。新增故障类型只需实现 Fault 子类并注册。"""

from .abort import AbortFault
from .base import Fault, FaultContext, FaultRegistry, fault
from .delay import DelayFault

__all__ = ["AbortFault", "Fault", "FaultContext", "FaultRegistry", "DelayFault", "fault"]
