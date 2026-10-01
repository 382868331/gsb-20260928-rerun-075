"""系统故障演练平台：场景编排、故障注入、观察记录与对照回放。"""

from .clock import VirtualClock
from .faults import (
    EffectHandle,
    FaultType,
    LatencyFault,
    InterruptionFault,
    TimeoutFault,
    FaultManager,
    fault_registry,
)
from .report import load_run, save_run
from .runner import ExperimentRunner, RunConfig, RunResult
from .scenario import InjectionSpec, RequestSpec, Scenario, load_scenario, save_scenario
from .service_chain import ServiceChain, ServiceNode

__all__ = [
    "VirtualClock",
    "EffectHandle",
    "FaultType",
    "LatencyFault",
    "InterruptionFault",
    "TimeoutFault",
    "FaultManager",
    "fault_registry",
    "ExperimentRunner",
    "RunConfig",
    "RunResult",
    "Scenario",
    "InjectionSpec",
    "RequestSpec",
    "load_scenario",
    "save_scenario",
    "ServiceChain",
    "ServiceNode",
    "save_run",
    "load_run",
]
