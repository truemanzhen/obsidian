# DLedger 模式

> 研究定位：把 Broker 侧启用 DLedger 后的落地形态讲清楚，回答“DLedger 模式下 CommitLog、角色和复制链到底怎么变”。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/dledger/DLedgerCommitLog.java`、相关 DLedger Store 实现
> 阅读建议：这篇是 Broker 存储侧 DLedger，不是 [[62-DLedger与Raft机制]] 里的 Controller 侧一致性。

## 先给结论

- DLedger 模式下，Broker 的物理日志实现会切到 DLedger 相关实现。
- 它和默认 `CommitLog + DefaultHAService` 不是同一条复制路径。
- 这条线更接近“复制日志 + 角色推进”的工程形态。

## `DLedgerCommitLog` 的意义

它直接继承 `CommitLog`，说明：

- 对上层来说它仍然是 CommitLog 语义
- 对下层来说它的日志组织和复制方式变了

所以 DLedger 模式本质上是：

- 替换底层日志实现

## 它和默认 HA 的区别

默认 HA 更偏：

- 主从复制
- 发送确认等待

DLedger 模式更偏：

- 一致性日志
- 角色与日志提交推进

所以两者不是简单开关差异，而是：

- 存储复制模型差异

## 这条线的研究重点

如果你要继续下钻，重点看：

- 日志如何复制
- leader 如何推进
- follower 如何跟随
- 角色变化如何影响消息写入和确认

## 哪些旧理解容易错

### “DLedger 模式只是换个 HA 协议”

不够准确。它会影响 commitlog 的实现形态。

### “DLedger 只管 controller”

不对。Broker 存储侧也有它的实现点。

## 这篇最值得记住的点

- DLedger 模式是 Broker 存储实现切换。
- 它和默认 HA 不是同一条链。
- `DLedgerCommitLog` 是最核心的落点类。

## 建议连读

1. [[62-DLedger与Raft机制]]
2. [[62-DLedger与Raft机制]]
3. [[61-HA服务实现细节]]