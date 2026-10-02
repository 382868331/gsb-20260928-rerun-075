"""自适应演练：按观测结果逐步调整干预，预算/风险上限，复演与阈值分支。

与首轮的区别：首轮的注入在时间线上预先固定；本模块不预先排定整段注入，
而是在每个观察到达时，依据当时已到达的观察与当时生效的阈值决定是否
升级下一阶段干预。每次决策都完整记录：决策时刻、窗口内观察、采用的
阈值、指标与所选动作。迟到的观察只影响其到达之后的决策，不会倒过来
改写先前决策记录。
"""

from dataclasses import asdict, dataclass, field
import heapq
import json
from pathlib import Path

from .faults import FaultManager, fault_registry
from .runner import Comparison, ExperimentRunner, Observation
from .scenario import InjectionSpec, Scenario, scenario_from_dict
from .service_chain import ServiceChain


@dataclass
class Thresholds:
    failure_rate: float = 0.5              # 窗口内未消费失败占比达到该值即升级
    latency_delta_ms: float | None = None  # 相对对照组的延迟增量阈值，None 不启用

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class LadderRung:
    """一级干预：升级时按阶梯顺序创建对应注入。"""

    type_name: str
    target: str
    duration_ms: int | None = None  # None 表示持续到演练结束
    params: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Budget:
    max_interventions: int = 3     # 预算：最多创建的干预次数
    max_failed_requests: int = 10  # 风险上限：失败请求达到该数即停止升级

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AdaptivePolicy:
    window_ms: int
    min_samples: int
    thresholds: Thresholds
    ladder: list[LadderRung]
    budget: Budget = field(default_factory=Budget)
    observation_delay_ms: int = 0  # 观察结果延迟到达，用于模拟迟到数据

    def to_dict(self) -> dict:
        return {
            "window_ms": self.window_ms,
            "min_samples": self.min_samples,
            "observation_delay_ms": self.observation_delay_ms,
            "thresholds": self.thresholds.to_dict(),
            "ladder": [r.to_dict() for r in self.ladder],
            "budget": self.budget.to_dict(),
        }


def policy_from_dict(data: dict) -> AdaptivePolicy:
    thresholds = Thresholds(**data.get("thresholds", {}))
    ladder = [LadderRung(**rung) for rung in data.get("ladder", [])]
    budget = Budget(**data.get("budget", {}))
    return AdaptivePolicy(
        window_ms=int(data["window_ms"]),
        min_samples=int(data["min_samples"]),
        thresholds=thresholds,
        ladder=ladder,
        budget=budget,
        observation_delay_ms=int(data.get("observation_delay_ms", 0)),
    )


@dataclass
class Decision:
    """一次决策的完整快照：保存后不可被后续迟到观察改写。"""

    seq: int
    decided_at_ms: int
    window_start_ms: int
    window_end_ms: int
    observation_ids: list[int]  # 决策时已到达且落入窗口的观察
    thresholds: dict            # 决策时采用的阈值
    metrics: dict               # 决策时算得的指标
    action: str                 # escalate / hold / undetermined / stop
    reason: str
    injection_id: str | None = None
    rung: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AdaptiveResult:
    scenario_name: str
    seed: int
    policy: AdaptivePolicy
    decisions: list[Decision]
    observations: list[Observation]          # 干预组
    control_observations: list[Observation]  # 不干预对照过程
    comparisons: list[Comparison]
    fault_events: list[dict]
    recovered: list[dict]
    stopped: bool
    stop_reason: str | None

    def to_dict(self) -> dict:
        return {
            "scenario_name": self.scenario_name,
            "seed": self.seed,
            "policy": self.policy.to_dict(),
            "stopped": self.stopped,
            "stop_reason": self.stop_reason,
            "decisions": [d.to_dict() for d in self.decisions],
            "observations": [o.to_dict() for o in self.observations],
            "control_observations": [o.to_dict() for o in self.control_observations],
            "comparisons": [c.to_dict() for c in self.comparisons],
            "fault_events": self.fault_events,
            "recovered": self.recovered,
        }


# 同刻处理顺序：先开始/结束注入，再处理请求，最后让观察到达。
_KIND_ORDER = {"inject_start": 0, "end": 1, "request": 2, "observe": 3}


class AdaptiveRunner:
    """观察驱动的逐步干预运行器。

    对照过程（不干预）与干预过程使用完全相同的输入序列与种子，
    恢复到底来自调整还是系统自身变化，可逐请求对比两者得出。
    """

    def __init__(self, chain: ServiceChain | None = None,
                 manager_factory=lambda: FaultManager(fault_registry)) -> None:
        self.chain = chain or ServiceChain()
        self.manager_factory = manager_factory

    def run(self, scenario: Scenario, policy: AdaptivePolicy) -> AdaptiveResult:
        control_obs = self._run_control(scenario)
        control_latency = {
            o.request_id: o.result["total_latency_ns"] for o in control_obs
        }

        manager = self.manager_factory()
        events: list[tuple] = []
        seq_counter = 0

        def push(at_ms: int, kind: str, payload: dict) -> None:
            nonlocal seq_counter
            heapq.heappush(
                events, (at_ms, _KIND_ORDER[kind], seq_counter, kind, payload))
            seq_counter += 1

        for idx, req in enumerate(scenario.requests):
            push(req.at_ms, "request", {"request_id": idx, "payload": req.payload})
        # 场景预置注入作为背景环境（如被演练系统本身的故障），
        # 与策略动态创建的干预共存于同一 FaultManager。
        for injection in scenario.injections:
            push(injection.start_ms, "inject_start", {"spec": injection})
            if injection.end_ms is not None:
                push(injection.end_ms, "end", {"id": injection.id})

        observations: list[Observation] = []
        available: list[Observation] = []  # 已到达的观察（决策只能看到这些）
        consumed: set[int] = set()         # 已推动过升级的失败/越限，不重复计数
        decisions: list[Decision] = []
        recovered: list[dict] = []
        interventions = 0
        rung = -1
        stopped = False
        stop_reason: str | None = None
        failed_total = 0

        while events:
            at_ms, _, _, kind, payload = heapq.heappop(events)
            if kind == "inject_start":
                manager.start(payload["spec"], at_ms)
            elif kind == "end":
                manager.end(payload["id"], at_ms)
            elif kind == "request":
                result = self.chain.call(
                    request_id=payload["request_id"],
                    payload=payload["payload"],
                    seed=scenario.seed,
                    manager=manager,
                )
                obs = Observation(
                    request_id=payload["request_id"],
                    group="fault",
                    scheduled_at_ms=at_ms,
                    result=result.to_dict(),
                )
                observations.append(obs)
                push(at_ms + policy.observation_delay_ms, "observe",
                     {"obs_index": len(observations) - 1})
            elif kind == "observe":
                obs = observations[payload["obs_index"]]
                available.append(obs)
                if obs.result["status"] != "ok":
                    failed_total += 1
                decision, created = self._decide(
                    policy=policy,
                    now_ms=at_ms,
                    available=available,
                    consumed=consumed,
                    control_latency=control_latency,
                    interventions=interventions,
                    rung=rung,
                    stopped=stopped,
                    stop_reason=stop_reason,
                    failed_total=failed_total,
                    seq=len(decisions),
                )
                decisions.append(decision)
                if created is not None:
                    spec, new_rung = created
                    manager.start(spec, at_ms)
                    interventions += 1
                    rung = new_rung
                    if spec.end_ms is not None:
                        push(spec.end_ms, "end", {"id": spec.id})
                if decision.action == "stop" and not stopped:
                    stopped = True
                    stop_reason = decision.reason

        # 达到停止条件后不再创建新干预；仍在生效的效果按其约定结束，
        # 未显式结束的由收尾统一撤销。
        finish_ms = (scenario.requests[-1].at_ms + policy.observation_delay_ms
                     if scenario.requests else 0)
        handles = manager.recover_all(finish_ms, reason="experiment_finished")
        recovered.extend(
            {"id": h.injection_id, "type": h.type_name,
             "target": h.target, "at_ms": finish_ms}
            for h in handles
        )
        if not manager.is_empty():
            raise RuntimeError("演练结束后仍有故障未恢复")

        comparisons = ExperimentRunner._compare(control_obs, observations)
        return AdaptiveResult(
            scenario_name=scenario.name,
            seed=scenario.seed,
            policy=policy,
            decisions=decisions,
            observations=observations,
            control_observations=control_obs,
            comparisons=comparisons,
            fault_events=manager.history,
            recovered=recovered,
            stopped=stopped,
            stop_reason=stop_reason,
        )

    def _run_control(self, scenario: Scenario) -> list[Observation]:
        observations: list[Observation] = []
        for idx, req in enumerate(scenario.requests):
            result = self.chain.call(
                request_id=idx, payload=req.payload,
                seed=scenario.seed, manager=None,
            )
            observations.append(Observation(
                request_id=idx, group="control",
                scheduled_at_ms=req.at_ms, result=result.to_dict(),
            ))
        return observations

    def _decide(self, *, policy, now_ms, available, consumed, control_latency,
                interventions, rung, stopped, stop_reason, failed_total,
                seq) -> tuple[Decision, tuple[InjectionSpec, int] | None]:
        window_start = now_ms - policy.window_ms
        window = [o for o in available
                  if window_start < o.scheduled_at_ms <= now_ms]
        obs_ids = [o.request_id for o in window]
        thresholds = policy.thresholds.to_dict()

        fresh = [o for o in window if o.request_id not in consumed]
        new_failures = [o for o in fresh if o.result["status"] != "ok"]
        latency_breach_ids: list[int] = []
        if policy.thresholds.latency_delta_ms is not None:
            limit_ns = policy.thresholds.latency_delta_ms * 1_000_000
            for o in fresh:
                delta = (o.result["total_latency_ns"]
                         - control_latency[o.request_id])
                if delta >= limit_ns:
                    latency_breach_ids.append(o.request_id)
        metrics = {
            "samples": len(window),
            "new_failures": len(new_failures),
            "failure_rate": (len(new_failures) / len(window)) if window else 0.0,
            "latency_breach_ids": latency_breach_ids,
            "failed_total": failed_total,
            "interventions": interventions,
        }

        def record(action: str, reason: str,
                   injection_id: str | None = None,
                   rung_no: int | None = None) -> Decision:
            return Decision(
                seq=seq, decided_at_ms=now_ms,
                window_start_ms=window_start, window_end_ms=now_ms,
                observation_ids=obs_ids, thresholds=thresholds,
                metrics=metrics, action=action, reason=reason,
                injection_id=injection_id, rung=rung_no,
            )

        # 资料不足：继续标为未确定，不做任何判断。
        if len(window) < policy.min_samples:
            return record("undetermined", "insufficient_samples"), None

        # 停止条件优先：达到预算或风险上限后不再创建新干预。
        if stopped:
            return record("stop", stop_reason), None
        if interventions >= policy.budget.max_interventions:
            return record("stop", "budget_exhausted"), None
        if failed_total >= policy.budget.max_failed_requests:
            return record("stop", "risk_limit_reached"), None

        breached = (metrics["failure_rate"] >= policy.thresholds.failure_rate
                    or bool(latency_breach_ids))
        if not breached:
            return record("hold", "thresholds_not_met"), None

        next_rung = rung + 1
        if next_rung >= len(policy.ladder):
            return record("hold", "ladder_exhausted"), None

        # 同一失败只推动一次升级：消费本窗口内所有越限观察。
        for o in new_failures:
            consumed.add(o.request_id)
        consumed.update(latency_breach_ids)

        step = policy.ladder[next_rung]
        spec = InjectionSpec(
            id=f"auto-{seq}",
            type_name=step.type_name,
            target=step.target,
            start_ms=now_ms,
            end_ms=None if step.duration_ms is None else now_ms + step.duration_ms,
            params=dict(step.params),
        )
        return record("escalate", "thresholds_met",
                      injection_id=spec.id, rung_no=next_rung), (spec, next_rung)


def save_adaptive_run(result: AdaptiveResult, scenario: Scenario,
                      path: str | Path, kind: str = "adaptive_run",
                      extra: dict | None = None) -> Path:
    path = Path(path)
    payload = {
        "kind": kind,
        "scenario": scenario.to_dict(),
        "policy": result.policy.to_dict(),
        "result": result.to_dict(),
    }
    if extra:
        payload.update(extra)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


def load_adaptive_run(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def replay_adaptive(saved: dict) -> AdaptiveResult:
    """按保存的场景与策略逐步复演原决策；原观察与动作不被覆盖。"""
    scenario = scenario_from_dict(saved["scenario"])
    policy = policy_from_dict(saved["policy"])
    return AdaptiveRunner().run(scenario, policy)


def run_branch(saved: dict, threshold_overrides: dict) -> tuple[AdaptiveResult, Scenario]:
    """用另一套阈值在独立分支上复演；原报告保持不变。"""
    scenario = scenario_from_dict(saved["scenario"])
    policy_data = dict(saved["policy"])
    thresholds = dict(policy_data.get("thresholds", {}))
    thresholds.update(threshold_overrides)
    policy_data["thresholds"] = thresholds
    policy = policy_from_dict(policy_data)
    result = AdaptiveRunner().run(scenario, policy)
    return result, scenario


def explain_branch(base: dict, branch: dict) -> list[dict]:
    """对比两个分支的决策序列，分别解释影响来源。

    首次分歧可明确归因于阈值差异（此前观察与动作完全一致）；
    其后的分歧记为上游分歧的下游影响。
    """
    base_decisions = base["result"]["decisions"]
    branch_decisions = branch["result"]["decisions"]
    overrides = branch.get("threshold_overrides", {})
    explanations: list[dict] = []
    diverged = False
    for seq, (base_d, branch_d) in enumerate(zip(base_decisions, branch_decisions)):
        same = (base_d["action"] == branch_d["action"]
                and base_d["injection_id"] == branch_d["injection_id"])
        if same:
            continue
        entry = {
            "seq": seq,
            "at_ms": base_d["decided_at_ms"],
            "base_action": base_d["action"],
            "branch_action": branch_d["action"],
            "metrics": base_d["metrics"],
        }
        if not diverged:
            diverged = True
            entry["cause"] = "threshold_change"
            entry["threshold_changes"] = [
                {
                    "threshold": key,
                    "base_value": base_d["thresholds"].get(key),
                    "branch_value": new_value,
                }
                for key, new_value in overrides.items()
            ]
        else:
            entry["cause"] = "downstream_of_divergence"
        explanations.append(entry)
    return explanations
