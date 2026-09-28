"""系统故障演练平台的行为测试。"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chaos_platform.cli import demo_scenario
from chaos_platform.faults import (
    AbortFault,
    DelayFault,
    Fault,
    FaultRegistry,
    fault,
)
from chaos_platform.runner import ExperimentRunner
from chaos_platform.scenario import (
    FaultSchedule,
    Scenario,
    ScenarioStore,
    uniform_request_sequence,
)
from chaos_platform.services import ServiceChain, ServiceRequest
from chaos_platform.clock import VirtualClock


SEED = 20260929


def requests(count=12):
    return uniform_request_sequence(count=count, interval_ms=10.0, seed=SEED)


class HealthyBaselineTests(unittest.TestCase):
    def test_control_and_fault_groups_identical_without_injection(self):
        scenario = Scenario(name="healthy", seed=SEED, requests=requests())
        result = ExperimentRunner(scenario).run()
        self.assertEqual(result.control.total, 12)
        self.assertEqual(result.fault_group.failed, 0)
        self.assertEqual(result.affected_requests, [])
        c = result.control.log.by_request()
        f = result.fault_group.log.by_request()
        for rid in c:
            self.assertEqual(c[rid].latency_ms, f[rid].latency_ms)
            self.assertEqual(c[rid].status, f[rid].status)


class DelayAndAbortTests(unittest.TestCase):
    def test_delay_adds_latency_only_within_window(self):
        scenario = Scenario(
            name="delay",
            seed=SEED,
            requests=requests(),
            injections=[
                FaultSchedule(
                    injection_id="d1",
                    type="delay",
                    start_ms=25,
                    end_ms=65,
                    service="service-b",
                    params={"delay_ms": 50.0},
                )
            ],
        )
        result = ExperimentRunner(scenario).run()
        diffs = {d.request_id: d for d in result.diffs}
        # 请求在 t=5,15,25,... 发出；窗口 [25,65) 影响 req-0002..0006。
        self.assertFalse(diffs["req-0001"].changed)
        for rid in ("req-0002", "req-0003", "req-0004", "req-0005"):
            self.assertEqual(diffs[rid].latency_delta_ms, 50.0)
            self.assertEqual(diffs[rid].fault_status, "ok")
        self.assertFalse(diffs["req-0007"].changed)

    def test_abort_changes_real_request_results(self):
        scenario = Scenario(
            name="abort",
            seed=SEED,
            requests=requests(),
            injections=[
                FaultSchedule(
                    injection_id="a1",
                    type="abort",
                    start_ms=25,
                    end_ms=45,
                    service="service-a",
                )
            ],
        )
        result = ExperimentRunner(scenario).run()
        diffs = {d.request_id: d for d in result.diffs}
        self.assertEqual(diffs["req-0002"].control_status, "ok")
        self.assertEqual(diffs["req-0002"].fault_status, "aborted@service-a")
        self.assertTrue(diffs["req-0002"].changed)
        self.assertEqual(result.control.failed, 0)
        self.assertEqual(result.fault_group.failed, 2)
        # 失败率来自真实结果统计
        self.assertEqual(result.fault_group.observed_failure_rate(), round(2 / 12, 4))
        self.assertFalse(diffs["req-0004"].changed)


class OverlapTests(unittest.TestCase):
    def test_ending_one_overlapped_injection_keeps_other_active(self):
        scenario = Scenario(
            name="overlap",
            seed=SEED,
            requests=requests(10),
            injections=[
                FaultSchedule(
                    injection_id="short",
                    type="delay",
                    start_ms=20,
                    end_ms=40,
                    service="service-b",
                    params={"delay_ms": 100.0},
                ),
                FaultSchedule(
                    injection_id="long",
                    type="delay",
                    start_ms=20,
                    end_ms=80,
                    service="database",
                    params={"delay_ms": 7.0},
                ),
            ],
        )
        result = ExperimentRunner(scenario).run()
        diffs = {d.request_id: d for d in result.diffs}
        # t=45 时 short 已结束，long 仍生效：只应保留 database 的 +7ms，
        # 不能把 long 一起撤掉（误撤则 delta=0）。
        self.assertEqual(diffs["req-0004"].latency_delta_ms, 7.0)
        # 重叠期内两跳都加延迟。
        self.assertEqual(diffs["req-0003"].latency_delta_ms, 107.0)
        # long 结束后恢复无差异。
        self.assertFalse(diffs["req-0008"].changed)


class CancelTests(unittest.TestCase):
    def test_cancel_revokes_all_still_active_faults(self):
        scenario = Scenario(
            name="cancel",
            seed=SEED,
            requests=requests(20),
            injections=[
                FaultSchedule(
                    injection_id="d1",
                    type="delay",
                    start_ms=20,
                    end_ms=500,
                    service="service-b",
                    params={"delay_ms": 99.0},
                ),
                FaultSchedule(
                    injection_id="a1",
                    type="abort",
                    start_ms=30,
                    end_ms=500,
                    service="gateway",
                ),
            ],
        )
        runner = ExperimentRunner(scenario)
        result = runner.run(cancel_after_ms=35)
        self.assertTrue(result.cancelled)
        self.assertEqual(sorted(result.active_on_cancel), ["a1", "d1"])
        self.assertEqual(sorted(result.restored_after_cancel), ["a1", "d1"])
        # 取消后不再发新请求
        self.assertLess(result.fault_group.total, 20)

    def test_cancel_flag_takes_effect_at_next_boundary(self):
        scenario = demo_scenario(SEED)
        runner = ExperimentRunner(scenario)
        runner.cancel()
        result = runner.run()
        self.assertTrue(result.cancelled)
        # 第一个事件（t=5 的首个请求）之前还没有注入生效
        self.assertEqual(result.restored_after_cancel, [])
        self.assertEqual(result.control.total, 0)


class ReplayTests(unittest.TestCase):
    def test_saved_seed_replays_identical_results(self, tmp_dir=None):
        import tempfile

        tmp = Path(tempfile.mkdtemp())
        scenario = demo_scenario(SEED)
        ScenarioStore(tmp).save(scenario)
        reloaded = ScenarioStore(tmp).load("demo-delay-abort")
        first = ExperimentRunner(scenario).run()
        second = ExperimentRunner(reloaded).run()
        payload = json.loads((tmp / "demo-delay-abort.json").read_text("utf-8"))
        self.assertEqual(payload["seed"], SEED)
        self.assertEqual(
            [d.__dict__ for d in first.diffs], [d.__dict__ for d in second.diffs]
        )
        self.assertEqual(first.summary(), second.summary())


class ExtensibilityTests(unittest.TestCase):
    def test_new_fault_type_without_touching_runner(self):
        @fault
        class PacketLossFault(Fault):
            type_name = "x_packet_loss"

            def apply_hop(self, ctx, hop):
                hop.injected_latency += 3.5

        self.assertIn("x_packet_loss", FaultRegistry.names())
        scenario = Scenario(
            name="ext",
            seed=SEED,
            requests=requests(2),
            injections=[
                FaultSchedule(
                    injection_id="p1",
                    type="x_packet_loss",
                    start_ms=0,
                    end_ms=100,
                    service="gateway",
                )
            ],
        )
        result = ExperimentRunner(scenario).run()
        # 自定义故障作用于 gateway 一跳（4 跳之一），每请求 +3.5ms
        self.assertEqual(result.diffs[0].latency_delta_ms, 3.5)


class LifecycleTests(unittest.TestCase):
    def test_restore_is_called_after_end(self):
        calls = []

        class TrackingFault(DelayFault):
            def on_start(self):
                calls.append("start")

            def on_end(self):
                calls.append("end")

            def on_restore(self):
                calls.append("restore")

        clock = VirtualClock()
        chain = ServiceChain(clock, SEED)
        from chaos_platform.manager import FaultManager

        manager = FaultManager()
        f1 = TrackingFault("t1", params={"delay_ms": 1.0})
        manager.start(f1)
        self.assertTrue(f1.started and not f1.ended)
        manager.end("t1")
        self.assertEqual(calls, ["start", "end", "restore"])
        self.assertTrue(f1.restored)
        with self.assertRaises(KeyError):
            manager.end("t1")


if __name__ == "__main__":
    unittest.main(verbosity=2)
