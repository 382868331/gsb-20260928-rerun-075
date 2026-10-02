# 系统故障演练平台

在项目自带的虚拟服务链（`gateway -> auth -> billing -> inventory -> notify`）上编排
静态注入与自适应干预：场景 → 观察到达 → 阈值决策 → 动作审计 → 对照比较 →
分支保存/逐步复演。

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
- **自适应决策**：`adaptive_policy.decision_at_ms` 定义决策点；决策只读取当时已完成
  的观察，保存窗口、阈值、新/旧失败、预算和所选动作。迟到结果不会改写先前审计。
- **预算与风险停止**：达到 `max_interventions` / `max_cost` / `max_risk` 后只阻止新干预，
  已选择的效果仍按 `duration_ms` / `end_ms` 结束，在途观察继续落地。
- **重叠观察去重与未确定**：同一失败被升级或因停止条件被记录后，不会在重叠窗口再次推动升级；
  样本数低于 `min_observations` 时持续标记为 `undetermined`。
- **独立阈值分支**：可用另一套阈值重跑相同输入和对照组，治疗观察、动作和解释独立保存，
  不覆盖原分支；解释区分干预、系统变化和自然失败。

## 目录结构

- `chaos_platform/service_chain.py`：虚拟服务链与请求结果。
- `chaos_platform/faults.py`：故障协议、注册表、内置类型、重叠/撤销管理。
- `chaos_platform/scenario.py`：场景模型与 JSON 读写、校验。
- `chaos_platform/runner.py`：时间线、取消恢复、双组对照与观察记录。
- `chaos_platform/adaptive.py`：到达即观察、阈值评估、预算/风险停止与分支运行。
- `chaos_platform/report.py` / `cli.py`：报告保存、重放校验、命令行。
- `examples/`：首轮延迟+中断场景与取消恢复场景。
- `tests/test_platform.py`：重叠撤销、取消恢复、对照比较、重放、扩展类型用例。

## 使用

```bash
.venv/bin/python -m chaos_platform faults
.venv/bin/python -m chaos_platform run examples/first_round.json -o /tmp/run.json
.venv/bin/python -m chaos_platform replay /tmp/run.json
.venv/bin/python -m chaos_platform adaptive-run examples/adaptive_demo.json \
  --thresholds examples/adaptive_thresholds_root.json \
  --alternate-thresholds examples/adaptive_thresholds_alt.json \
  -o /tmp/adaptive.json
.venv/bin/python -m chaos_platform replay /tmp/adaptive.json
.venv/bin/python -m pytest -q
```

场景 JSON 中 `injections[].type` 引用注册表名称，`end_ms` 为 `null` 表示持续到
实验结束或 `cancel_at_ms` 取消；新增故障类型后只需在场景中引用其类型名。

自适应场景使用 `adaptive_policy`：`actions.degraded/failed` 指定候选故障、成本和风险；
阈值由命令行 JSON 提供。报告中的每个 `decisions[]` 都是不可变快照，重放时按这些决策点
重新推导；根分支和 `alternate` 分支分别保留自己的观察、动作与影响解释。
