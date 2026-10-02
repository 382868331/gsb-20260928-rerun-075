"""命令行入口：run / replay / faults / adaptive。"""

import argparse
import json
from pathlib import Path

from .adaptive import (
    AdaptiveRunner,
    explain_branch,
    load_adaptive_run,
    policy_from_dict,
    replay_adaptive,
    run_branch,
    save_adaptive_run,
)
from .faults import fault_registry
from .report import load_run, save_run
from .runner import ExperimentRunner
from .scenario import scenario_from_dict, load_scenario


def _print_summary(result_dict: dict) -> None:
    result = result_dict["result"]
    print(f"场景: {result['scenario_name']}  种子: {result['seed']}")
    if result["cancelled"]:
        print(f"实验在 {result['cancel_at_ms']}ms 被取消，撤销故障: "
              + ", ".join(r["id"] for r in result["recovered"]))
    affected = [c for c in result["comparisons"]
                if c["status_changed"] or c["latency_delta_ns"] != 0]
    print(f"请求数: {len(result['comparisons'])}，受影响: {len(affected)}")
    for comp in affected:
        delta_ms = comp["latency_delta_ns"] / 1_000_000
        note = comp["fault_status"] if comp["status_changed"] else f"+{delta_ms:g}ms"
        print(f"  #{comp['request_id']} @{comp['at_ms']}ms: "
              f"{comp['control_status']} -> {note}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chaos", description="系统故障演练平台")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="运行场景（对照组 + 故障组）")
    run_p.add_argument("scenario", type=Path)
    run_p.add_argument("-o", "--output", type=Path, help="保存完整报告的 JSON 路径")

    replay_p = sub.add_parser("replay", help="用已保存报告中的种子与输入重放并比对")
    replay_p.add_argument("report", type=Path)

    sub.add_parser("faults", help="列出已注册故障类型")

    adaptive_p = sub.add_parser("adaptive", help="观察驱动的逐步干预演练")
    adaptive_sub = adaptive_p.add_subparsers(dest="adaptive_command", required=True)

    a_run = adaptive_sub.add_parser("run", help="按策略逐步调整干预（含对照过程）")
    a_run.add_argument("scenario", type=Path, help="含 policy 字段的场景 JSON")
    a_run.add_argument("-o", "--output", type=Path, help="保存完整报告的 JSON 路径")

    a_replay = adaptive_sub.add_parser("replay", help="逐步复演已保存的决策")
    a_replay.add_argument("report", type=Path)

    a_branch = adaptive_sub.add_parser("branch", help="用另一套阈值建立独立分支")
    a_branch.add_argument("report", type=Path)
    a_branch.add_argument("--set", dest="overrides", action="append",
                          default=[], metavar="KEY=VALUE",
                          help="覆盖阈值，如 failure_rate=0.8，可多次")
    a_branch.add_argument("-o", "--output", type=Path, required=True,
                          help="分支报告保存路径（原报告不被修改）")

    a_explain = adaptive_sub.add_parser("explain", help="对比两个分支并解释影响来源")
    a_explain.add_argument("base", type=Path)
    a_explain.add_argument("branch", type=Path)
    args = parser.parse_args(argv)

    if args.command == "faults":
        for name in fault_registry.names():
            print(name)
        return 0

    if args.command == "run":
        scenario = load_scenario(args.scenario)
        result = ExperimentRunner().run(scenario)
        if args.output:
            save_run(result, scenario, args.output)
            print(f"报告已保存: {args.output}")
        _print_summary({"result": result.to_dict()})
        return 0

    if args.command == "replay":
        saved = load_run(args.report)
        scenario = scenario_from_dict(saved["scenario"])
        result = ExperimentRunner().run(scenario).to_dict()
        if result != saved["result"]:
            print("重放结果与保存报告不一致")
            return 1
        print("重放结果与保存报告完全一致")
        _print_summary(saved)
        return 0

    if args.command == "adaptive":
        return _adaptive_main(args)
    return 0


def _parse_overrides(pairs: list[str]) -> dict:
    overrides: dict = {}
    for pair in pairs:
        key, _, value = pair.partition("=")
        if not key or not value:
            raise SystemExit(f"无效的阈值覆盖: {pair!r}，应为 KEY=VALUE")
        overrides[key] = float(value)
    return overrides


def _print_decisions(result: dict) -> None:
    print(f"场景: {result['scenario_name']}  种子: {result['seed']}"
          + (f"  已停止: {result['stop_reason']}" if result["stopped"] else ""))
    for d in result["decisions"]:
        extra = f" -> {d['injection_id']}(rung {d['rung']})" if d["injection_id"] else ""
        print(f"  决策#{d['seq']} @{d['decided_at_ms']}ms: {d['action']}"
              f"（{d['reason']}）样本={d['metrics']['samples']}"
              f" 失败率={d['metrics']['failure_rate']:.2f}{extra}")


def _adaptive_main(args) -> int:
    if args.adaptive_command == "run":
        data = json.loads(args.scenario.read_text(encoding="utf-8"))
        scenario = scenario_from_dict(data)
        policy = policy_from_dict(data["policy"])
        result = AdaptiveRunner().run(scenario, policy)
        if args.output:
            save_adaptive_run(result, scenario, args.output)
            print(f"报告已保存: {args.output}")
        _print_decisions(result.to_dict())
        return 0

    if args.adaptive_command == "replay":
        saved = load_adaptive_run(args.report)
        result = replay_adaptive(saved).to_dict()
        if result != saved["result"]:
            print("复演结果与保存报告不一致")
            return 1
        print("复演结果与保存报告完全一致")
        _print_decisions(saved["result"])
        return 0

    if args.adaptive_command == "branch":
        saved = load_adaptive_run(args.report)
        overrides = _parse_overrides(args.overrides)
        result, scenario = run_branch(saved, overrides)
        save_adaptive_run(result, scenario, args.output, kind="adaptive_branch",
                          extra={"threshold_overrides": overrides})
        print(f"分支报告已保存: {args.output}（原报告未修改）")
        _print_decisions(result.to_dict())
        return 0

    if args.adaptive_command == "explain":
        base = load_adaptive_run(args.base)
        branch = load_adaptive_run(args.branch)
        explanations = explain_branch(base, branch)
        if not explanations:
            print("两个分支的决策完全一致")
            return 0
        for e in explanations:
            print(f"决策#{e['seq']} @{e['at_ms']}ms: "
                  f"{e['base_action']} -> {e['branch_action']}（{e['cause']}）")
            for change in e.get("threshold_changes", []):
                print(f"  阈值 {change['threshold']}: "
                      f"{change['base_value']} -> {change['branch_value']}")
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
