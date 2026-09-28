"""系统故障演练平台：实验场景、故障注入与观察记录。

核心概念
--------
* :class:`~chaos_platform.services.ServiceChain`：项目自带的虚拟服务链。
* :class:`~chaos_platform.faults.base.Fault`：故障类型基类，新故障只需注册，
  不必改动运行控制（:mod:`chaos_platform.runner`）。
* :class:`~chaos_platform.scenario.Scenario`：实验场景，描述输入序列与注入计划。
* :class:`~chaos_platform.runner.ExperimentRunner`：驱动开始/结束/恢复、
  对照组与故障组对比、取消撤销以及种子重放。
"""

from .clock import VirtualClock
from .faults.base import Fault, FaultContext, FaultRegistry, fault
from .faults.delay import DelayFault
from .faults.abort import AbortFault
from .recorder import Observation, ObservationLog, RequestResult
from .runner import ExperimentRunner, ExperimentResult, GroupRun
from .scenario import FaultSchedule, Scenario, ScenarioStore
from .services import ServiceChain, ServiceRequest

__all__ = [
    "AbortFault",
    "DelayFault",
    "ExperimentResult",
    "ExperimentRunner",
    "Fault",
    "FaultContext",
    "FaultRegistry",
    "FaultSchedule",
    "GroupRun",
    "Observation",
    "ObservationLog",
    "RequestResult",
    "Scenario",
    "ScenarioStore",
    "ServiceChain",
    "ServiceRequest",
    "VirtualClock",
    "fault",
]
