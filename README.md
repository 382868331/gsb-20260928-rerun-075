# 系统故障演练平台

在项目自带的虚拟服务链（`gateway -> auth -> billing -> inventory -> notify`）上
编排故障注入实验：场景 → 注入 → 观察记录 → 对照比较 → 保存/重放。

## 设计要点

- **运行控制与故障类型解耦**：故障类型实现 `FaultType.create_handle()` 后注册到
  `fault_registry`（见 `chaos_platform/faults.py`），内置 `latency`（延迟）、
  `interruption`（中断）与 `timeout`（超时）。后续新增丢包、错误码、带宽限制等类型无需改
  `ExperimentRunner`。
- **完整生命周期**：每个注入有 `start_ms` / `end_ms`；未显式结束的注入在实验结束或
  取消时由 `FaultManager.recover_all()` 撤销。取消时仍生效的故障全部恢复。
- **重叠注入不互撤**：效果按 `(作用维度, 目标服务)` 堆叠，按注入 ID 精确移除。
  例如两个延迟同时作用于 `billing` 时叠加，先结束其中一个只减去自己的延迟。
- **故障依赖链**：注入可声明 `depends_on`（依赖的注入须正在生效）与
  `start_when`（观察条件，如 `latency_at_least` / `failures_at_least`）。
  带条件的注入进入待定状态，条件满足才生效；窗口（`end_ms`）已过仍未满足则记为
  `inject_skipped`。典型用法：延迟积累触发超时，超时再影响带 `retries` 的请求重试。
- **提前停止**：`stop_when` 支持 `consecutive_failures` 与
  `failure_rate_at_least`。停止在每个请求处理完后评估，因此同刻请求照常完成，
  之后时刻的请求一律记为 `not_executed`，仍生效的故障按 `early_stop` 撤销。
- **同刻顺序规则**：`inject_start → inject_end/cancel → request → finish`。
  取消与请求同刻时先撤销故障，该请求按无故障完成。
- **真实结果对照，而非故障率**：同一场景跑两遍——对照组无故障、故障组启用注入，
  两者使用完全相同的输入序列与种子，逐请求比较真实状态（`ok/interrupted`）、
  延迟增量和失败节点。
- **影响归因**：每次请求对照给出 `attributed_to`——造成延迟增量或失败的注入 ID
  列表（含各次重试），最终影响可追溯到具体注入而非只有总失败率。
- **确定性与重放**：服务链每一跳的自然延迟只由 `(seed, 请求序号, 跳序号)` 决定，
  虚拟时钟推进，不做真实 sleep。报告保存种子、完整输入与逐事件日志
  （`event_log`），重放时逐事件比对，不一致会指出第一处分叉。

## 目录结构

- `chaos_platform/service_chain.py`：虚拟服务链与请求结果。
- `chaos_platform/faults.py`：故障协议、注册表、内置类型、重叠/撤销管理。
- `chaos_platform/scenario.py`：场景模型与 JSON 读写、校验。
- `chaos_platform/runner.py`：时间线、取消恢复、双组对照与观察记录。
- `chaos_platform/report.py` / `cli.py`：报告保存、重放校验、命令行。
- `examples/`：延迟+中断、取消恢复、依赖链（延迟→超时→重试）、提前停止场景。
- `tests/test_platform.py`：重叠撤销、取消恢复、依赖触发、提前停止、同刻规则、
  归因、逐事件重放、扩展类型用例。

## 使用

```bash
.venv/bin/python -m chaos_platform faults
.venv/bin/python -m chaos_platform run examples/first_round.json -o /tmp/run.json
.venv/bin/python -m chaos_platform replay /tmp/run.json
.venv/bin/python -m pytest -q
```

场景 JSON 中 `injections[].type` 引用注册表名称，`end_ms` 为 `null` 表示持续到
实验结束或 `cancel_at_ms` 取消；新增故障类型后只需在场景中引用其类型名。
