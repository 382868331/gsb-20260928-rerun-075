"""实验场景：同一输入序列 + 一组带生命周期的故障注入。"""

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path


@dataclass
class RequestSpec:
    at_ms: int
    payload: dict = field(default_factory=dict)
    retries: int = 0  # 失败（中断/超时）后在同一时刻立即重试的次数上限


@dataclass
class InjectionSpec:
    id: str
    type_name: str
    target: str
    start_ms: int
    end_ms: int | None = None  # None 表示持续到取消/恢复
    params: dict = field(default_factory=dict)
    # 依赖与触发条件：为空时按 start_ms 直接生效；
    # 否则进入待定状态，满足全部条件后才生效（条件型注入）。
    depends_on: list[str] = field(default_factory=list)  # 这些注入须处于生效中
    start_when: dict | None = None  # 观察条件，见 validate_scenario

    @property
    def conditional(self) -> bool:
        return bool(self.depends_on) or self.start_when is not None


@dataclass
class Scenario:
    name: str
    seed: int
    requests: list[RequestSpec]
    injections: list[InjectionSpec] = field(default_factory=list)
    cancel_at_ms: int | None = None
    stop_when: list[dict] = field(default_factory=list)  # 提前停止的观察条件
    description: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "seed": self.seed,
            "cancel_at_ms": self.cancel_at_ms,
            "stop_when": self.stop_when,
            "requests": [asdict(r) for r in self.requests],
            "injections": [
                {
                    "id": i.id,
                    "type": i.type_name,
                    "target": i.target,
                    "start_ms": i.start_ms,
                    "end_ms": i.end_ms,
                    "params": i.params,
                    "depends_on": i.depends_on,
                    "start_when": i.start_when,
                }
                for i in self.injections
            ],
        }


_START_WHEN_KINDS = {"latency_at_least", "failures_at_least"}
_STOP_WHEN_KINDS = {"consecutive_failures", "failure_rate_at_least"}


def scenario_from_dict(data: dict) -> Scenario:
    requests = [
        RequestSpec(at_ms=int(r["at_ms"]), payload=r.get("payload", {}),
                    retries=int(r.get("retries", 0)))
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
            depends_on=list(i.get("depends_on", [])),
            start_when=i.get("start_when"),
        )
        for i in data.get("injections", [])
    ]
    scenario = Scenario(
        name=data["name"],
        description=data.get("description", ""),
        seed=int(data["seed"]),
        cancel_at_ms=(None if data.get("cancel_at_ms") is None
                      else int(data["cancel_at_ms"])),
        stop_when=list(data.get("stop_when", [])),
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
        for dep in injection.depends_on:
            if dep == injection.id:
                raise ValueError(f"注入 {injection.id} 不能依赖自身")
        if injection.start_when is not None:
            _validate_condition(injection.start_when, _START_WHEN_KINDS,
                                f"注入 {injection.id} 的 start_when")
    for injection in scenario.injections:
        for dep in injection.depends_on:
            if dep not in ids:
                raise ValueError(f"注入 {injection.id} 依赖不存在的注入 {dep}")
    for cond in scenario.stop_when:
        _validate_condition(cond, _STOP_WHEN_KINDS, "stop_when 条件")
    for request in scenario.requests:
        if request.retries < 0:
            raise ValueError("retries 必须是非负整数")
    request_times = [r.at_ms for r in scenario.requests]
    if request_times != sorted(request_times):
        raise ValueError("请求序列必须按 at_ms 升序排列")


def _validate_condition(cond: dict, kinds: set[str], where: str) -> None:
    if not isinstance(cond, dict) or cond.get("kind") not in kinds:
        raise ValueError(f"{where} 的 kind 必须是 {sorted(kinds)} 之一")


def save_scenario(scenario: Scenario, path: str | Path) -> Path:
    path = Path(path)
    path.write_text(
        json.dumps(scenario.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return path


def load_scenario(path: str | Path) -> Scenario:
    return scenario_from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
