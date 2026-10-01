"""命令行入口：run / replay / faults。"""

import argparse
import json
from pathlib import Path

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
    if result.get("stopped"):
        print(f"实验在 {result['stopped_at_ms']}ms 提前停止（{result['stop_reason']}），"
              f"撤销故障: " + ", ".join(r["id"] for r in result["recovered"]))
    affected = [c for c in result["comparisons"]
                if c["status_changed"] or c["latency_delta_ns"] != 0]
    print(f"请求数: {len(result['comparisons'])}，受影响: {len(affected)}")
    for comp in affected:
        delta_ms = comp["latency_delta_ns"] / 1_000_000
        note = comp["fault_status"] if comp["status_changed"] else f"+{delta_ms:g}ms"
        sources = ", ".join(comp.get("attributed_to", [])) or "无"
        print(f"  #{comp['request_id']} @{comp['at_ms']}ms: "
              f"{comp['control_status']} -> {note}  (来源注入: {sources})")


def _first_divergence(saved: dict, replayed: dict) -> str:
    """逐事件比对，返回第一处不一致的描述。"""
    saved_events = saved.get("event_log", [])
    new_events = replayed.get("event_log", [])
    for idx, (old, new) in enumerate(zip(saved_events, new_events)):
        if old != new:
            return (f"事件 #{idx} 不一致:\n  保存: {old}\n  重放: {new}")
    if len(saved_events) != len(new_events):
        return f"事件数量不一致: 保存 {len(saved_events)}，重放 {len(new_events)}"
    saved_obs = saved.get("observations", [])
    new_obs = replayed.get("observations", [])
    for idx, (old, new) in enumerate(zip(saved_obs, new_obs)):
        if old != new:
            return f"观察记录 #{idx} (request {old.get('request_id')}) 不一致"
    return "结果字段不一致（事件与观察记录均相同）"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chaos", description="系统故障演练平台")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="运行场景（对照组 + 故障组）")
    run_p.add_argument("scenario", type=Path)
    run_p.add_argument("-o", "--output", type=Path, help="保存完整报告的 JSON 路径")

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

    if args.command == "replay":
        saved = load_run(args.report)
        scenario = scenario_from_dict(saved["scenario"])
        result = ExperimentRunner().run(scenario).to_dict()
        if result != saved["result"]:
            print("重放结果与保存报告不一致")
            print(_first_divergence(saved["result"], result))
            return 1
        print("重放结果与保存报告完全一致")
        for event in saved["result"].get("event_log", []):
            print(f"  [{event['seq']:>3}] @{event['at_ms']}ms {event['kind']}"
                  + "".join(f" {k}={v}" for k, v in event.items()
                            if k not in ("seq", "at_ms", "kind")))
        _print_summary(saved)
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
