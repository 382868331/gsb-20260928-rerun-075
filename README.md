# 系统故障演练平台（首轮）

在项目自带的虚拟服务链（`gateway -> auth -> billing -> inventory -> notify`）上
编排故障注入实验：场景 → 注入 → 观察记录 → 对照比较 → 保存/重放。

## 设计要点

- **运行控制与故障类型解耦**：故障类型实现 `FaultType.create_handle()` 后注册到
  `fault_registry`（见 `chaos_platform/faults.py`），首轮内置 `latency`（延迟）与
  `interruption`（中断）。后续新增丢包、错误码、带宽限制等类型无需改
  `ExperimentRunner`。
- **完整生命周期**：每个注入有 `start_ms` / `end_ms`；未显式结束的注入在实验结束或
  取消时由 `FaultManager.recover_all()` 撤销。取消时仍生效的故障全部恢复。
- **重叠注入不互撤**：效果按 `(作用维度, 目标服务)` 堆叠，按注入 ID 精确移除。
  例如两个延迟同时作用于 `billing` 时叠加，先结束其中一个只减去自己的延迟。
- **真实结果对照，而非故障率**：同一场景跑两遍——对照组无故障、故障组启用注入，
  两者使用完全相同的输入序列与种子，逐请求比较真实状态（`ok/interrupted`）、
  延迟增量和失败节点。
- **确定性与重放**：服务链每一跳的自然延迟只由 `(seed, 请求序号, 跳序号)` 决定，
  虚拟时钟推进，不做真实 sleep。报告保存种子与完整输入，可原样重放并逐字段比对。

## 目录结构

- `chaos_platform/service_chain.py`：虚拟服务链与请求结果。
- `chaos_platform/faults.py`：故障协议、注册表、内置类型、重叠/撤销管理。
- `chaos_platform/scenario.py`：场景模型与 JSON 读写、校验。
 - `chaos_platform/runner.py`：时间线、取消恢复、双组对照与观察记录。
 - `chaos_platform/adaptive.py`：观察驱动的逐步干预、预算/风险上限、复演与阈值分支。
 - `chaos_platform/report.py` / `cli.py`：报告保存、重放校验、命令行。
 - `examples/`：首轮场景与次轮自适应场景。
 - `tests/`：首轮平台用例与自适应演练用例。

## 次轮：自适应逐步干预

首轮的注入在时间线上预先固定；次轮（`chaos_platform/adaptive.py`）不预先排定
整段注入，而是在每个观察到达时按当时已到达的观察与当时生效的阈值决定是否
升级下一阶段干预：

- **决策快照不可改写**：每次决策记录决策时刻、窗口内观察、采用的阈值、指标与
  所选动作（`escalate/hold/undetermined/stop`）。`observation_delay_ms` 模拟
  迟到结果，迟到数据只影响其到达之后的决策，不会倒过来改写先前记录。
- **对照过程**：不干预的对照组与干预组使用相同输入与种子并行记录，恢复来自
  调整还是系统自身变化可逐请求对比。
- **预算与风险上限**：`budget.max_interventions` / `max_failed_requests` 触发
  后不再创建新干预；仍在生效的效果按各自 `duration_ms` 结束，在途请求照常
  完成，未显式结束的由收尾统一撤销。
- **重叠窗口去重**：窗口内已推动过升级的失败/越限会被消费，同一失败不会重复
  推动升级；样本不足 `min_samples` 时决策标为 `undetermined`。
- **复演与分支**：`adaptive replay` 按保存的场景与策略逐步复演原决策并逐字段
  比对；`adaptive branch --set 阈值=新值` 建立独立分支（原报告不被修改），
  `adaptive explain` 将首次分歧归因于阈值变化，后续分歧标记为上游分歧的下游
  影响。

```bash
.venv/bin/python -m chaos_platform adaptive run examples/adaptive_demo.json -o /tmp/ad.json
.venv/bin/python -m chaos_platform adaptive replay /tmp/ad.json
.venv/bin/python -m chaos_platform adaptive branch /tmp/ad.json --set latency_delta_ms=1000 -o /tmp/ad_branch.json
.venv/bin/python -m chaos_platform adaptive explain /tmp/ad.json /tmp/ad_branch.json
```

## 使用

```bash
.venv/bin/python -m chaos_platform faults
.venv/bin/python -m chaos_platform run examples/first_round.json -o /tmp/run.json
.venv/bin/python -m chaos_platform replay /tmp/run.json
.venv/bin/python -m pytest -q
```

场景 JSON 中 `injections[].type` 引用注册表名称，`end_ms` 为 `null` 表示持续到
实验结束或 `cancel_at_ms` 取消；新增故障类型后只需在场景中引用其类型名。
