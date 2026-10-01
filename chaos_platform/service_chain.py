"""项目自带的虚拟服务链：网关 -> 鉴权 -> 计费 -> 库存 -> 通知。"""

from dataclasses import dataclass, field
import random


class ServiceError(Exception):
    """服务链自身产生的错误。"""


@dataclass(frozen=True)
class ServiceNode:
    name: str
    base_latency_ms: tuple[int, int] = (2, 8)


@dataclass(frozen=True)
class HopRecord:
    node: str
    latency_ns: int


@dataclass
class CallResult:
    request_id: int
    payload: dict
    status: str  # ok / error / interrupted / timeout
    total_latency_ns: int
    hops: list[HopRecord] = field(default_factory=list)
    failed_node: str | None = None
    error: str | None = None
    # 归因：本次调用中各注入贡献的附加延迟，以及导致失败的注入。
    contributions: dict[str, int] = field(default_factory=dict)
    failed_by: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "payload": self.payload,
            "status": self.status,
            "total_latency_ns": self.total_latency_ns,
            "hops": [{"node": h.node, "latency_ns": h.latency_ns} for h in self.hops],
            "failed_node": self.failed_node,
            "error": self.error,
            "contributions": self.contributions,
            "failed_by": self.failed_by,
        }


DEFAULT_CHAIN = [
    ServiceNode("gateway", (1, 3)),
    ServiceNode("auth", (2, 6)),
    ServiceNode("billing", (4, 12)),
    ServiceNode("inventory", (3, 9)),
    ServiceNode("notify", (1, 4)),
]


@dataclass
class ServiceChain:
    """按固定链路依次调用；故障管理器决定每一跳的附加延迟与中断。"""

    nodes: list[ServiceNode] = field(default_factory=lambda: list(DEFAULT_CHAIN))

    def node_names(self) -> list[str]:
        return [node.name for node in self.nodes]

    def call(
        self,
        request_id: int,
        payload: dict,
        seed: int,
        manager: "FaultManager | None",
    ) -> CallResult:
        hops: list[HopRecord] = []
        total_ns = 0
        contributions: dict[str, int] = {}
        for hop_index, node in enumerate(self.nodes):
            # 仅由 (种子, 请求序号, 跳序号) 决定：对照组/故障组/重放完全一致。
            rng = random.Random(f"{seed}:{request_id}:{hop_index}")
            low, high = node.base_latency_ms
            latency_ns = rng.randint(low, high) * 1_000_000
            if manager is not None:
                for source_id, added_ns in manager.latency_contributions(node.name).items():
                    latency_ns += added_ns
                    contributions[source_id] = contributions.get(source_id, 0) + added_ns
                # 同跳先判超时（累计耗时超阈值），再判中断（连接被重置）。
                timed_out = [
                    source_id
                    for source_id, limit_ns in manager.timeout_limits(node.name).items()
                    if latency_ns > limit_ns
                ]
                interrupted = manager.interruption_sources(node.name)
                if timed_out or interrupted:
                    failed_by = timed_out if timed_out else interrupted
                    status = "timeout" if timed_out else "interrupted"
                    hops.append(HopRecord(node.name, latency_ns))
                    total_ns += latency_ns
                    return CallResult(
                        request_id=request_id,
                        payload=payload,
                        status=status,
                        total_latency_ns=total_ns,
                        hops=hops,
                        failed_node=node.name,
                        error=(f"deadline exceeded at {node.name}"
                               if timed_out else
                               f"connection reset by fault injection at {node.name}"),
                        contributions=contributions,
                        failed_by=failed_by,
                    )
            hops.append(HopRecord(node.name, latency_ns))
            total_ns += latency_ns
        return CallResult(
            request_id=request_id,
            payload=payload,
            status="ok",
            total_latency_ns=total_ns,
            hops=hops,
            contributions=contributions,
        )
