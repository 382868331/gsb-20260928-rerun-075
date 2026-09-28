"""实验场景：同一输入序列 + 一组带生命周期的故障注入。"""

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path


@dataclass
class RequestSpec:
    at_ms: int
    payload: dict = field(default_factory=dict)


@dataclass
class InjectionSpec:
    id: str
    type_name: str
    target: str
    start_ms: int
    end_ms: int | None = None  # None 表示持续到取消/恢复
    params: dict = field(default_factory=dict)

@dataclass
class Scenario:
    name: str
    seed: int
    requests: list[RequestSpec]
    injections: list[InjectionSpec] = field(default_factory=list)
    cancel_at_ms: int | None = None
    description: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "seed": self.seed,
            "cancel_at_ms": self.cancel_at_ms,
            "requests": [asdict(r) for r in self.requests],
            "injections": [
                {
                    "id": i.id,
                    "type": i.type_name,
                    "target": i.target,
                    "start_ms": i.start_ms,
                    "end_ms": i.end_ms,
                    "params": i.params,
                }
                for i in self.injections
            ],
        }


def scenario_from_dict(data: dict) -> Scenario:
    requests = [
        RequestSpec(at_ms=int(r["at_ms"]), payload=r.get("payload", {}))
        for r in data["requests"]
    ]
    injections = [
        InjectionSpec(
            id=i["id"],
            type_name=i["type"],
            target=i["target"],
            start_ms=int(i["start_ms"]),
            end_ms=None if i.get("end_ms") is None else int(i["end_ms"]),
            params=i.get("params", {}),
        )
        for i in data.get("injections", [])
    ]
    scenario = Scenario(
        name=data["name"],
        description=data.get("description", ""),
        seed=int(data["seed"]),
        cancel_at_ms=(None if data.get("cancel_at_ms") is None
                      else int(data["cancel_at_ms"])),
        requests=requests,
        injections=injections,
    )
    validate_scenario(scenario)
    return scenario


def validate_scenario(scenario: Scenario) -> None:
    ids: set[str] = set()
    for injection in scenario.injections:
        if injection.id in ids:
            raise ValueError(f"注入 ID 重复: {injection.id}")
        ids.add(injection.id)
        if injection.end_ms is not None and injection.end_ms < injection.start_ms:
            raise ValueError(f"注入 {injection.id} 的 end_ms 早于 start_ms")
    request_times = [r.at_ms for r in scenario.requests]
    if request_times != sorted(request_times):
        raise ValueError("请求序列必须按 at_ms 升序排列")


def save_scenario(scenario: Scenario, path: str | Path) -> Path:
    path = Path(path)
    path.write_text(
        json.dumps(scenario.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return path


def load_scenario(path: str | Path) -> Scenario:
    return scenario_from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
