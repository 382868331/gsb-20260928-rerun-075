"""命令行入口：run / replay / faults。"""

import argparse
import json
from pathlib import Path

from .faults import fault_registry
from .report import load_run, save_adaptive_run, save_run
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


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _print_adaptive_summary(result: dict) -> None:
    print(f"自适应场景: {result['scenario_name']}  种子: {result['seed']}")
    for branch in result["branches"]:
        selected = [d for d in branch["decisions"]
                    if d.get("action") and d["action"].get("selected")]
        print(f"分支 {branch['branch_id']}: 决策 {len(branch['decisions'])} 次，"
              f"选择干预 {len(selected)} 次，停止: {branch['stopped']}"
              + (f"（{branch['stop_reason']}）" if branch['stop_reason'] else ""))
        for decision in selected:
            action = decision["action"]["spec"]
            print(f"  @{decision['at_ms']}ms {decision['severity']}: "
                  f"{action['id']} -> {action['target']} {action['type']}")
        suggestions = branch.get("suggestions", [])
        if suggestions:
            counts: dict[str, int] = {}
            for suggestion in suggestions:
                counts[suggestion["status"]] = \
                    counts.get(suggestion["status"], 0) + 1
            detail = ", ".join(f"{k}:{v}" for k, v in sorted(counts.items()))
            print(f"  建议 {len(suggestions)} 条（{detail}）")
        for bundle in branch.get("bundles", []):
            print(f"  组合 @{bundle['at_ms']}ms [{bundle['mode']}] "
                  f"{bundle['status']}: 生效 {len(bundle.get('effective_suggestion_ids', []))}"
                  f"/{len(bundle['suggestion_ids'])}")
        for plan in branch.get("plans", []):
            spent = plan["spent"]
            print(f"  方案 {plan['id']} @{plan['at_ms']}ms "
                  f"[{plan['strategy']}] {plan['status']}: "
                  f"生效 {len(plan.get('executed_suggestion_ids', []))}"
                  f"/{len(plan['suggestion_ids'])}，"
                  f"支出 cost={spent['cost']:g} risk={spent['risk']:g}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chaos", description="系统故障演练平台")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="运行场景（对照组 + 故障组）")
    run_p.add_argument("scenario", type=Path)
    run_p.add_argument("-o", "--output", type=Path, help="保存完整报告的 JSON 路径")

    adaptive_p = sub.add_parser("adaptive-run", help="按观察结果逐步选择干预")
    adaptive_p.add_argument("scenario", type=Path)
    adaptive_p.add_argument("--thresholds", required=True, type=Path)
    adaptive_p.add_argument("--alternate-thresholds", type=Path)
    adaptive_p.add_argument("--approvals", type=Path,
                            help="根分支人工确认/放弃/撤回事件 JSON")
    adaptive_p.add_argument("--alternate-approvals", type=Path)
    adaptive_p.add_argument("-o", "--output", type=Path)

    replay_p = sub.add_parser("replay", help="用已保存报告中的种子与输入重放并比对")
    replay_p.add_argument("report", type=Path)

    sub.add_parser("faults", help="列出已注册故障类型")
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

    if args.command == "adaptive-run":
        scenario = load_scenario(args.scenario)
        thresholds = _load_json(args.thresholds)
        alternate = (_load_json(args.alternate_thresholds)
                     if args.alternate_thresholds else None)
        approvals = _load_json(args.approvals) if args.approvals else None
        alt_approvals = (_load_json(args.alternate_approvals)
                         if args.alternate_approvals else None)
        result = ExperimentRunner().run_adaptive_experiment(
            scenario, thresholds, alternate,
            approvals=approvals, alternate_approvals=alt_approvals)
        if args.output:
            save_adaptive_run(result, scenario, args.output)
            print(f"自适应报告已保存: {args.output}")
        _print_adaptive_summary(result)
        return 0

    if args.command == "replay":
        saved = load_run(args.report)
        scenario = scenario_from_dict(saved["scenario"])
        if saved.get("schema") == "adaptive_run_v1":
            runner = ExperimentRunner()
            current = {
                "scenario_name": scenario.name,
                "seed": scenario.seed,
                "branches": [
                    runner.run_adaptive_branch(scenario, branch["thresholds"],
                                               branch["branch_id"],
                                               approvals=branch.get("approvals"))
                    for branch in saved["result"]["branches"]
                ],
            }
            if current != saved["result"]:
                print("自适应重放结果与保存报告不一致")
                return 1
            print("自适应重放结果与保存报告完全一致")
            _print_adaptive_summary(saved["result"])
            return 0
        result = ExperimentRunner().run(scenario).to_dict()
        if result != saved["result"]:
            print("重放结果与保存报告不一致")
            return 1
        print("重放结果与保存报告完全一致")
        _print_summary(saved)
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
