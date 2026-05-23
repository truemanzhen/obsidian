# ReputMessageService 异步分发

> 研究定位：把 CommitLog 和逻辑索引之间的桥梁讲清楚，回答“为什么写完 CommitLog 之后还要再跑一个分发线程”。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java` 内部的 `ReputMessageService`、`CommitLogDispatcherBuildConsumeQueue`、`CommitLogDispatcherBuildIndex`、`CommitLogDispatcherBuildTransIndex`
> 阅读建议：这篇要和 [[28-CommitLog写入流程]]、[[29-ConsumeQueue设计]] 连读。

## 先给结论

- `ReputMessageService` 是后台常驻线程，职责是从 CommitLog 里不断读取新增内容并分发。
- 它把“物理写入完成”转换成“逻辑索引可消费”。
- 分发本身是异步的，所以消费者看到的新消息一定晚于 CommitLog 写入时刻。
- 5.5.0 的分发对象不止 ConsumeQueue，还包括 Index 和事务相关索引。

## 它解决的核心问题

如果没有这层分发：

- CommitLog 虽然已经写入，但 Consumer 仍无法按 topic-queue 顺序读
- key 索引也无法建立
- 事务消息的辅助结构也无法同步更新

所以 `ReputMessageService` 本质上是：

- 把物理日志转换成多种逻辑视图的后台派发器

## 主循环的意义

它从 `reputFromOffset` 位置开始，循环做三件事：

1. [[28-CommitLog写入流程]]读取 CommitLog 数据
2. [[29-ConsumeQueue设计]]解析消息边界
3. [[32-IndexFile索引设计]] [[32-IndexFile索引设计]]分发给不同 dispatcher

这意味着：

- CommitLog 负责“写进去”
- ReputMessageService 负责“把能读的索引补齐”

## 三个 dispatcher

### 1. [[28-CommitLog写入流程]]`CommitLogDispatcherBuildConsumeQueue`

负责写入消费索引。

它只处理已经提交的普通消息和事务提交消息，不处理半消息和回滚消息。

### 2. [[29-ConsumeQueue设计]]`CommitLogDispatcherBuildIndex`

负责构建 key / uniqKey / tag 索引。

这部分是否生效受 `IndexService` 开关控制。

### 3. [[32-IndexFile索引设计]] [[32-IndexFile索引设计]]`CommitLogDispatcherBuildTransIndex`

负责事务消息的辅助索引链。

在 5.5.0 里，事务消息不再只靠单一队列逻辑解释，而是有专门的事务存储支线。

## 为什么它必须异步

异步分发不是“为了好看”，而是为了避免把索引构建阻塞在主写入路径上。

如果同步做完所有索引构建：

- CommitLog 写入延迟会被放大
- 发送吞吐会明显下降

所以 RocketMQ 的策略是：

- 主写入先完成
- 索引随后跟进

这就是典型的“写前台、索引后台”架构。

## 分发延迟意味着什么

写入 CommitLog 和消费者真正可读之间，存在一个短暂窗口。

这个窗口不是 bug，而是架构选择：

- 主链路快
- 索引异步补齐

所以如果你看见：

- CommitLog 已写入
- 但 ConsumeQueue 还没就绪

这通常只是分发滞后，而不是消息丢了。

## `reputFromOffset` 的边界

这个偏移不是“消费者 offset”，而是：

- 当前分发线程已经追到 CommitLog 的哪个位置

它会随着分发推进不断移动。

如果它落后太多，就说明：

- 索引构建在积压
- 消费端可能暂时读不到最新消息

## 为什么分发线程要单线程

ReputMessageService 保持单线程分发，是为了保证：

- 消息顺序
- 索引构建顺序
- 简化与 CommitLog 读取位置的协调

如果多线程并行构建索引，就会引入额外的排序和一致性成本。

## 这篇最值得记住的边界

- CommitLog 是事实源。
- ReputMessageService 是逻辑索引构建器。
- ConsumeQueue / Index 不是写入主路径的同步结果，而是异步派生结果。

## 和后续笔记的关系

-  解释 key 索引是怎么被构建出来的。
- [[33-消息存储清理与回溯边界]] 解释分发和清理如何共同影响可见窗口。

## 建议连读

1. [[28-CommitLog写入流程]]
2. [[29-ConsumeQueue设计]]
3. [[32-IndexFile索引设计]]