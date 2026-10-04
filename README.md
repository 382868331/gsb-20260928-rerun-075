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
- **人工确认干预**：`adaptive_policy.approval.required` 开启后，决策只产生
  **建议（pending）**，由 `approvals` 事件（`confirm`/`abandon`/`withdraw`）驱动：
  确认时按当时的风险余额、在生效干预和停止条件重新检查，过期建议
  （`expires_after_ms`）不能绕过已触发的停止；同一（维度, 目标）范围被占用时
  确认被暂缓（`scope_contention`），须等前一项结束或撤回后重试。放弃未生效建议
  不改写历史观察；撤回已生效干预按原收尾约定撤销，并记录
  `affected_observation_ids`。未获批准的建议不会出现在 `fault_events` 中。
- **相互依赖建议的组合批准**：审批事件可用 `confirm_bundle` 一次性确认一组建议，
  并显式给出 `suggestion_ids` 与 `mode`：`all` 表示全部条件同时满足才允许执行
  （任一成员在执行前复核中失效——过期、已非 pending、范围被占用或组合预算不足
  ——则整组不执行，且**针对旧条件的整组确认不会被应用到变化后的剩余建议**，
  失效成员也不会被自动替换成其他动作）；`partial` 表示允许只执行仍通过复核的成员，
  失效成员被跳过并记录原因。`abandon_bundle` 放弃整组尚未生效的成员。
  执行前对每名成员复核**有效期、影响范围与组合预算**并保存 `precheck` 快照；
  partial 模式执行途中若预算/风险停止触发，已生效成员的事实保留，组合状态记为
  `partially_effective`，不会整体记成未执行。每条建议保留决策依据观察 `basis`、
  人工确认 `events` 与执行前复核；放弃组合与重新确认组合可作为两个独立分支比较，
  结果随报告保存并可重放（见报告中的 `bundles[]` 与 `tests/test_bundles.py`）。

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
.venv/bin/python -m chaos_platform adaptive-run examples/adaptive_approval.json \
  --thresholds examples/adaptive_approval_thresholds.json \
  --approvals examples/adaptive_approvals_root.json \
  -o /tmp/adaptive_approval.json
.venv/bin/python -m chaos_platform adaptive-run examples/adaptive_approval.json \
  --thresholds examples/adaptive_approval_thresholds.json \
  --approvals examples/adaptive_approvals_bundle.json \
  -o /tmp/adaptive_bundle.json
.venv/bin/python -m chaos_platform replay /tmp/adaptive.json
.venv/bin/python -m pytest -q
```

场景 JSON 中 `injections[].type` 引用注册表名称，`end_ms` 为 `null` 表示持续到
实验结束或 `cancel_at_ms` 取消；新增故障类型后只需在场景中引用其类型名。

自适应场景使用 `adaptive_policy`：`actions.degraded/failed` 指定候选故障、成本和风险；
阈值由命令行 JSON 提供。报告中的每个 `decisions[]` 都是不可变快照，重放时按这些决策点
重新推导；根分支和 `alternate` 分支分别保留自己的观察、动作与影响解释。
