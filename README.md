# 系统故障演练平台（Chaos Drill Platform）

纯 Python 标准库实现，围绕三件事组织：**实验场景 → 故障注入 → 观察记录**。
首轮在项目自带的虚拟服务链（gateway → service-a → service-b → database）上
注入**延迟**与**中断**；后续新增故障类型只需注册，不必改动运行控制。

## 快速开始

```bash
# 内置演示（含重叠延迟 + 中断），保存种子以便重放
python -m chaos_platform demo --seed 20260929 --save-dir ./scenarios

# 按保存的种子逐毫秒重放，结果完全一致
python -m chaos_platform replay ./scenarios demo-delay-abort

# 在指定时刻取消实验，仍生效的故障会被撤销
python -m chaos_platform demo --seed 20260929 --cancel-after-ms 55

# 运行自定义场景文件
python -m chaos_platform run ./scenarios/demo-delay-abort.json

# 查看已注册故障类型
python -m chaos_platform faults
```

运行测试：

```bash
python -m unittest discover -s tests
```

## 模块划分

- `chaos_platform/scenario.py`：场景模型。输入序列、注入计划（开始/结束时间）、
  随机种子，可序列化为 JSON；`ScenarioStore` 负责保存与加载。
- `chaos_platform/services.py`：虚拟服务链。逐跳处理真实请求，
  每跳基线延迟按“种子 + 请求ID + 跳”确定性派生。
- `chaos_platform/faults/`：故障框架。`Fault` 基类定义
  `on_start / apply_hop / on_end / on_restore` 生命周期，`FaultRegistry` 注册表
  提供按名创建；内置 `delay`（延迟）与 `abort`（中断）。
- `chaos_platform/manager.py`：注入管理器。按 `injection_id` 独立维护活跃注入，
  保证重叠注入结束其一不会误撤另一项；`cancel_all` 用于实验取消。
- `chaos_platform/runner.py`：运行控制。时间轴事件驱动
  （故障开始/结束、请求派发），对照组与故障组使用**同一输入序列、同一种子**，
  逐请求比较真实结果。
- `chaos_platform/recorder.py`：观察记录与差异对比，失败率由真实请求结果统计，
  不是预设的固定“故障率”。

## 关键语义

- **生命周期**：`start → apply_hop... → end → restore`。正常结束走 `end`，
  实验取消时对全部仍生效注入执行 `end + restore`，取消后系统恢复健康。
- **重叠注入**：每项注入是独立实例，结束/恢复只影响自身 ID。
- **对照组对比**：两组共享同一种子与输入，基线抖动完全一致，差异只能来自故障。
- **重放**：种子与场景保存为 JSON，重放得到完全相同的逐请求结果。
- **时间模型**：虚拟时钟，无真实等待；请求按派发时刻的活跃故障集合执行。

## 新增故障类型

运行控制不感知具体故障，新增类型只需实现并注册：

```python
from chaos_platform.faults import Fault, fault

@fault
class PacketLossFault(Fault):
    type_name = "packet_loss"

    def apply_hop(self, ctx, hop):
        hop.injected_latency += 5.0
```

随后场景 JSON 中即可使用 `"type": "packet_loss"`，运行、取消、重放、
对照组对比逻辑全部复用，无需修改 `runner`。
