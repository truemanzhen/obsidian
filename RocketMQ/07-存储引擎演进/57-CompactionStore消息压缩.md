# CompactionStore 消息压缩

> 研究定位：纠正“CompactionStore = 消息体压缩”的常见误解，回答它在 RocketMQ 5.5.0 里到底扮演什么角色。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/kv/CompactionStore.java`、`CompactionService.java`、`CompactionLog.java`、`CompactionPositionMgr.java`
> 阅读建议：这篇里的“压缩”指的是 compaction/归并，不是 [[43-消息压缩机制]] 里的 codec 压缩。

## 先给结论

- CompactionStore 不是对消息体做 gzip/zstd，那是另一条链。
- 它是按 key 做归并保留的独立存储分支，主要服务 `CleanupPolicy.COMPACTION` 主题。
- 5.5.0 里它挂接在 `DefaultMessageStore` 旁边，由 `CommitLogDispatcherCompaction` 把满足条件的消息分发进去。
- 它有自己的 log、consume queue、position manager 和定时 compaction 任务。

## 为什么叫 compaction

这里的 compaction 更接近：

- 同一 key 多个版本只保留最新语义

而不是：

- 对单条消息体做算法压缩

所以理解这篇时要把两个“压缩”彻底分开：

- codec compression：节省体积
- key compaction：压缩版本历史

## 它挂在 `DefaultMessageStore` 的哪里

5.5.0 里 `DefaultMessageStore` 会在启动阶段构建：

- `CompactionStore`
- `CompactionService`
- `CommitLogDispatcherCompaction`

这说明 CompactionStore 不是外部插件，而是 store 体系的一条可选分支。

## 哪些 topic 会进入 CompactionStore

不是所有 topic 默认都走这条线。

`CompactionStore.scanAllTopicConfig()` 会扫描 topic 配置，并基于：

- `CleanupPolicy.COMPACTION`

来决定是否为该 topic/queue 创建 `CompactionLog`。

所以它是：

- 主题级能力

而不是全局统一开关。

## 核心对象分工

### `CompactionStore`

总控对象，负责：

- topic 扫描
- `CompactionLog` 生命周期
- 调度 compaction 任务

### `CompactionService`

负责接收来自 CommitLog 分发链的消息，并决定是否进入 compaction 分支。

### `CompactionLog`

是单个 topic-queue 级别的 compaction 存储体。

它内部还有：

- 自己的 mapped file queue
- 稀疏 consume queue
- compaction 执行逻辑

### `CompactionPositionMgr`

负责持久化和恢复 compaction 处理进度。

## 为什么它还要有自己的 CQ

因为 compaction store 不是“只留一个值塞到 map 里”，它仍需要：

- 消费读取
- offset 组织
- 落盘恢复

所以它会维护：

- `compactionLog`
- `compactionCq`

两套文件目录。

## compaction 任务怎么运行

`CompactionStore` 会为每个 `CompactionLog` 按固定周期调度：

- `doCompaction()`

并且还会有随机延迟抖动，避免所有队列同一时刻同时 compaction。

这说明 compaction 不是“写一条就立刻全局重写”，而是：

- 后台周期性归并

## 它和普通消费链的关系

CompactionStore 不取代普通 CommitLog / ConsumeQueue，而是为某类 topic 提供另一种读取语义。

所以更准确的理解是：

- 默认 store 负责完整历史
- compaction store 负责按 key 归并后的视图

## 哪些旧理解最容易错

### “CompactionStore 是消息体压缩”

不对。那属于 codec 压缩。

### “CompactionStore 独立于 CommitLog”

不对。它的数据入口仍来自 CommitLog 分发链。

### “开启 compaction 后就没有 offset 语义了”

不对。它依然维护自己的 cq 和消费读取结构。

## 这篇最值得记住的点

- CompactionStore 是按 key 归并的独立 store 分支。
- 只有 `CleanupPolicy.COMPACTION` 的 topic 才会走这条线。
- 它与 [[43-消息压缩机制]] 解决的完全不是一个问题。

## 建议连读

1. [[43-消息压缩机制]]
2. [[55-RocksDB集成设计]]
3. [[58-冷数据消费控制]]