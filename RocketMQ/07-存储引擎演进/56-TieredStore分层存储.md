# TieredStore 分层存储

> 研究定位：把 TieredStore 从“冷热分层概念图”落到 5.5.0 真实插件入口、读写分工与回退边界上。
> 关键源码：`tieredstore/README.md`、`tieredstore/.../TieredMessageStore.java`、`tieredstore/.../core/MessageStoreDispatcherImpl.java`、`tieredstore/.../core/MessageStoreFetcherImpl.java`
> 阅读建议：先和 [[55-RocksDB集成设计]] 区分。RocksDB 讨论的是本地存储底座演进；TieredStore 讨论的是插件式冷数据卸载与读取回退。

## 先给结论

- TieredStore 在 5.5.0 里不是把它塞进 `storeType` 并列切换的写法。
- 它的真实启用方式是 `messageStorePlugIn=org.apache.rocketmq.tieredstore.TieredMessageStore`。
- `TieredMessageStore` 继承的是 `AbstractPluginMessageStore`，本质是包在默认 store 外面的插件层。
- 写入链路不是“直接跳过本地磁盘写远端”，而是通过 dispatcher 把默认存储里的数据异步分发到 tiered store。
- 读取链路也不是永远只读远端；它会按 `tieredStorageLevel` 判断是否优先走 tiered，再在必要时 fallback 到 next store。

## 入口长什么样

`tieredstore/README.md` 的 quick start 很明确：

1. [[55-RocksDB集成设计]]配 `messageStorePlugIn=org.apache.rocketmq.tieredstore.TieredMessageStore`
2. [[58-冷数据消费控制]]配 `tieredBackendServiceProvider`
3. 配 `tieredStoreFilePath`

这说明 TieredStore 的真实形态是：

- 一层 message store plugin
- 可换不同 backend provider

而不是单纯改一个 `storeType` 枚举值。

## 核心对象

### `TieredMessageStore`

类定义：

```java
public class TieredMessageStore extends AbstractPluginMessageStore
```

内部关键成员：

- `defaultStore`
- `metadataStore`
- `flatFileStore`
- `indexService`
- `fetcher`
- `dispatcher`

这几个成员已经把真实架构说清楚了：

- `defaultStore` 仍然存在
- tiered store 额外维护自己的 metadata / flat file / fetch / dispatch 体系

### `dispatcher`

`TieredMessageStore` 构造时会：

```java
this.dispatcher = new MessageStoreDispatcherImpl(this);
next.addDispatcher(dispatcher);
```

这意味着 tiered store 是挂在“默认存储写入后的分发点”上工作的，不是独占写路径。

### `fetcher`

读路径由 `MessageStoreFetcherImpl` 承担，并维护：

- `MetadataStore`
- `FlatFileStore`
- `fetcherCache`

说明 tiered store 读远端/冷端时还有自己的预读缓存和 metadata 查询逻辑。

## 写路径的真实语义

`MessageStoreDispatcherImpl.dispatch(...)` 首先只做一件很轻的事：

```java
flatFileStore.computeIfAbsent(new MessageQueue(...));
```

真正的数据推进发生在后续 `doScheduleDispatch(...)`。

几个关键点：

- 它会拿 `defaultStore.getMinOffsetInQueue(...) / getMaxOffsetInQueue(...)`
- 初始化 flat file 时，不是从 0 开始，而是按时间窗口选一个起点
- 提交过程里还会处理 failed group commit 重试

这说明 TieredStore 更像：

- 从默认 store 中“搬运”和“索引化”已有数据
- 再形成自己的 flat file 体系

而不是把 RocketMQ 改造成另一套完全平行的写入存储。

## 读路径的真实语义

### 是否读 tiered，不是绝对的

`TieredMessageStore.fetchFromCurrentStore(...)` 会根据 `tieredStorageLevel` 判断是否优先走 tiered。

可见的等级包括：

- `DISABLE`
- `NOT_IN_DISK`
- `NOT_IN_MEM`
- `FORCE`

这几个值说明它不是固定的“冷热三层自动搬运图”，而是更偏向“在什么条件下从 tiered store 读”。

### 读取后可回退到 next store

`getMessageAsync(...)` 里如果 tiered 返回：

- `OFFSET_FOUND_NULL`
- `NO_MATCHED_LOGIC_QUEUE`

且 next store 仍然能覆盖这个 offset，就会 fallback：

```java
return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
```

这说明：

- TieredStore 不是单向替换
- 它和默认 store 之间存在显式回退语义

### fetcher 有读前缓存

`MessageStoreFetcherImpl` 使用 Caffeine 维护：

```java
Cache<String, SelectBufferResult> fetcherCache
```

并按 `readAheadCacheExpireDuration`、`readAheadCacheSizeThresholdRate` 控制生命周期和内存占用。

因此 tiered read 的性能模型里，不只是后端 provider，还包括本地 read-ahead cache。

## metadata 不是附属细节

`TieredMessageStore` 初始化 metadata 的方式是：

```java
Class.forName(storeConfig.getTieredMetadataServiceProvider())
```

默认实现来自：

- `DefaultMetadataStore`

它负责持有：

- topic / queue 对应的 flat file 信息
- file segment metadata

研究时要意识到，TieredStore 的可用性很大程度上依赖 metadata 的一致性，而不是只看“远端对象存储是否可用”。

## FileSegmentType 说明了它管哪些数据

TieredStore 内部的 `FileSegmentType` 包括：

- `COMMIT_LOG`
- `CONSUME_QUEUE`
- `INDEX`

所以它并不是“只给 CommitLog 冷存”，而是对消息读取链条上几个关键文件类型都做了 tiered 化建模。

## 和 RocksDB 的边界

这两者经常一起出现，但不是一回事。

### RocksDB

- 仍然是 Broker 本地存储底座的一部分
- 主要讨论 consume queue、index、timer、transaction state 的 RocksDB 化

### TieredStore

- 是 message store plugin
- 目标是把消息数据从本地磁盘卸载到更便宜、更大的后端
- 通过 dispatcher/fetcher/metadata 形成一层读写外挂

所以：

- RocksDB 讨论“本地怎么存”
- TieredStore 讨论“本地之外怎么卸载、怎么读回”

## 旧理解里最该修正的地方

- 不是 `MessageStoreConfig.storeType` 上的并列枚举切换
- 不是单纯“三层热温冷磁盘图”就能概括实现
- 不是没有默认 store 的独立消息引擎
- 不是所有读请求都直接走 tiered provider

这些都会掩盖 5.5.0 真实的插件式结构。

## 建议继续阅读

- [[58-冷数据消费控制]]
- [[26-存储架构总览]]