# DLedger 与 Raft 机制

> 研究定位：把 RocketMQ 5.5.0 里 DLedger、Raft 和 Controller 的关系拆开，回答“它们到底是一个东西，还是几条不同层次的链”。
> 关键源码：`controller/src/main/java/org/apache/rocketmq/controller/impl/DLedgerController.java`、`DLedgerControllerStateMachine.java`、`store/src/main/java/org/apache/rocketmq/store/dledger/DLedgerCommitLog.java`
> 阅读建议：这篇讲的是一致性机制与其在 RocketMQ 中的落点，不等于默认 HA。

## 先给结论

- DLedger 是 RocketMQ 在一致性复制上的重要实现载体，但不等于整个高可用体系。
- Raft 是背后的共识思想，DLedger 是 RocketMQ/阿里系实现语境里的工程化承载。
- 在 5.5.0 里，你会同时看到：
  - Broker 存储侧的 `DLedgerCommitLog`
  - Controller 侧的 `DLedgerController`
- 所以“DLedger”在 RocketMQ 中不是单点功能，而是跨控制面和数据面出现的实现形态。

## 为什么它和默认 HA 不同

默认 HA 的核心是：

- 主从复制
- 发送确认

它并不解决：

- 谁来选主
- 如何保证副本集元数据的一致推进

而 DLedger / Raft 关注的是：

- 日志复制一致性
- leader/follower 角色
- term/epoch 推进

## Controller 侧的 DLedger

`DLedgerController` 和 `DLedgerControllerStateMachine` 说明：

- Controller 自己也可以跑在一致性日志之上

这条线解决的是：

- 副本集元数据变更
- elect master
- sync state set 等控制面状态

它不是存储 CommitLog 本体，而是存储：

- Controller 的元数据决策日志

## Broker 存储侧的 DLedger

`DLedgerCommitLog` 继承自 `CommitLog`。

这说明：

- Broker 的物理日志实现可以被替换成基于 DLedger 的复制日志模型

所以它和默认 `CommitLog + DefaultHAService` 的区别在于：

- 日志复制语义更靠近共识日志

## 为什么这篇要同时提 Raft

因为很多工程名词会让人误以为：

- DLedger 是一套和 Raft 无关的专有魔法

更准确的理解是：

- Raft 提供了 leader、term、日志复制和多数派提交的抽象
- DLedger 是 RocketMQ 里采用或贴近这类模型的实现

## 5.5.0 里最容易混淆的两层

### 1. [[60-Controller与AutoSwitch主线]]控制面一致性

- `DLedgerController`
- Controller 状态机

### 2. [[63-DLedger模式]]数据面一致性

- `DLedgerCommitLog`

如果只看其中一层，会误解另一层的职责。

## 这篇最值得记住的边界

- DLedger 不是默认 HA 的别名。
- Controller 和 Broker 都可能用到 DLedger 形态，但职责不同。
- Raft 是一致性思想，DLedger 是具体工程实现载体。

## 和后续笔记的关系

-  继续看 Broker 启用 DLedger 模式后的实现形态。
-  负责 Controller 切主与元数据主线。

## 建议连读

1. [[60-Controller与AutoSwitch主线]]
2. [[63-DLedger模式]]
3. [[64-EscapeBridge容错桥接]]