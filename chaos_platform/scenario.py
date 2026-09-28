"""实验场景：输入序列 + 故障注入计划 + 种子，可序列化为 JSON 重放。"""

from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .services import DEFAULT_TOPOLOGY, ServiceRequest


@dataclass
class FaultSchedule:
    """一条注入计划：在 start_ms 开始、end_ms 结束。"""

    injection_id: str
    type: str
    start_ms: float
    end_ms: float
    service: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if self.end_ms <= self.start_ms:
            raise ValueError(
                f"注入 {self.injection_id} 的结束时间必须晚于开始时间"
            )


@dataclass
class Scenario:
    name: str
    seed: int
    requests: List[ServiceRequest]
    injections: List[FaultSchedule] = field(default_factory=list)
    topology: tuple[str, ...] = DEFAULT_TOPOLOGY
    duration_ms: Optional[float] = None

    def __post_init__(self) -> None:
        for item in self.injections:
            item.validate()

    @property
    def end_time_ms(self) -> float:
        planned = [s.end_ms for s in self.injections]
        if self.duration_ms is not None:
            planned.append(self.duration_ms)
        return max(planned, default=0.0)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "seed": self.seed,
            "topology": list(self.topology),
            "duration_ms": self.duration_ms,
            "requests": [asdict(r) for r in self.requests],
            "injections": [asdict(i) for i in self.injections],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Scenario":
        requests = [ServiceRequest(**item) for item in data.get("requests", [])]
        injections = [FaultSchedule(**item) for item in data.get("injections", [])]
        topology = tuple(data.get("topology", DEFAULT_TOPOLOGY))
        return cls(
            name=data["name"],
            seed=int(data["seed"]),
            requests=requests,
            injections=injections,
            topology=topology,
            duration_ms=data.get("duration_ms"),
        )

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return path

    @classmethod
    def load(cls, path: str | Path) -> "Scenario":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(data)


class ScenarioStore:
    """场景文件的保存/加载入口（保存种子后即可重放）。"""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def save(self, scenario: Scenario) -> Path:
        return scenario.save(self.directory / f"{scenario.name}.json")

    def load(self, name: str) -> Scenario:
        return Scenario.load(self.directory / f"{name}.json")

    def names(self) -> list[str]:
        return sorted(p.stem for p in self.directory.glob("*.json"))


def uniform_request_sequence(
    count: int, interval_ms: float = 10.0, seed: Optional[int] = None
) -> List[ServiceRequest]:
    """生成等间隔输入序列；payload_size 可用种子产生确定性变化。"""
    rng = random.Random(seed if seed is not None else 0)
    return [
        ServiceRequest(
            request_id=f"req-{i:04d}",
            path="/api/query",
            payload_size=rng.randint(1, 8),
        )
        for i in range(count)
    ]


def new_seed() -> int:
    return random.Random().randrange(1, 2**31)
