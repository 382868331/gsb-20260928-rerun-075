"""命令行入口：内置延迟/中断演示场景，可保存、重放并打印对比。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Optional

from .faults import AbortFault, DelayFault  # noqa: F401  确保内置类型已注册
from .runner import ExperimentRunner
from .scenario import FaultSchedule, Scenario, ScenarioStore, new_seed
from .scenario import uniform_request_sequence


def demo_scenario(seed: Optional[int] = None) -> Scenario:
    """首轮场景：延迟与中断，且两项延迟注入在时间窗上重叠。"""
    seed = seed if seed is not None else new_seed()
    requests = uniform_request_sequence(count=20, interval_ms=10.0, seed=seed)
    injections: List[FaultSchedule] = [
        FaultSchedule(
            injection_id="delay-b",
            type="delay",
            start_ms=20.0,
            end_ms=80.0,
            service="service-b",
            params={"delay_ms": 50.0},
        ),
        FaultSchedule(
            injection_id="delay-db",
            type="delay",
            start_ms=50.0,
            end_ms=120.0,
            service="database",
            params={"delay_ms": 30.0},
        ),
        FaultSchedule(
            injection_id="abort-a",
            type="abort",
            start_ms=100.0,
            end_ms=140.0,
            service="service-a",
        ),
    ]
    return Scenario(
        name="demo-delay-abort",
        seed=seed,
        requests=requests,
        injections=injections,
        duration_ms=200.0,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chaos", description="系统故障演练平台")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="运行内置延迟/中断演示")
    demo.add_argument("--seed", type=int, default=None, help="随机种子")
    demo.add_argument("--save-dir", type=Path, default=None, help="保存场景目录")
    demo.add_argument("--cancel-after-ms", type=float, default=None, help="在此时间取消")

    run = sub.add_parser("run", help="运行已保存的场景文件")
    run.add_argument("scenario_file", type=Path)
    run.add_argument("--cancel-after-ms", type=float, default=None)

    replay = sub.add_parser("replay", help="按种子重放目录中的场景")
    replay.add_argument("store_dir", type=Path)
    replay.add_argument("name")

    sub.add_parser("faults", help="列出已注册故障类型")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "faults":
        from .faults import FaultRegistry

        print("\n".join(FaultRegistry.names()))
        return 0

    if args.command == "demo":
        scenario = demo_scenario(args.seed)
        if args.save_dir is not None:
            path = ScenarioStore(args.save_dir).save(scenario)
            print(f"场景已保存（种子={scenario.seed}）：{path}")
        result = ExperimentRunner(scenario).run(cancel_after_ms=args.cancel_after_ms)
    elif args.command == "run":
        scenario = Scenario.load(args.scenario_file)
        result = ExperimentRunner(scenario).run(cancel_after_ms=args.cancel_after_ms)
    else:
        scenario = ScenarioStore(args.store_dir).load(args.name)
        result = ExperimentRunner(scenario).run()

    print(json.dumps(result.summary(), ensure_ascii=False, indent=2))
    if result.cancelled:
        print(f"实验已取消，撤销仍生效的注入：{result.restored_after_cancel}")
    for diff in result.affected_requests[:10]:
        print(
            f"{diff.request_id}: {diff.control_status} -> {diff.fault_status}, "
            f"延迟变化 {diff.latency_delta_ms:+.3f}ms"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
