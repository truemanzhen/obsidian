# RocksDB 集成设计

> 研究定位：澄清 5.5.0 里的 RocksDB 到底替换了什么、没替换什么，以及它怎样嵌进 `DefaultMessageStore`。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/RocksDBMessageStore.java`、`DefaultMessageStore.java`、`queue/RocksDBConsumeQueueStore.java`、`rocksdb/MessageRocksDBStorage.java`
> 阅读建议：先看本篇，再接 [[56-TieredStore分层存储]]、[[57-CompactionStore消息压缩]]、[[41-延迟消息Store层]]。

## 先给结论

- 5.5.0 不是“把整个 MessageStore 改成 RocksDB 版”。
- 真正的结构是：`RocksDBMessageStore extends DefaultMessageStore`，只把某些子系统换成 RocksDB。
- 其中最明确的一层是 ConsumeQueue；另外 timer、事务状态、索引也能接到 RocksDB。

## 第一处容易误判：storeType 的真实值

源码定义的 `StoreType` 只有两个值：

```java
public enum StoreType {
    DEFAULT("default"),
    DEFAULT_ROCKSDB("defaultRocksDB");
}
```

`MessageStoreConfig` 也是按这个值判断：

```java
private String storeType = StoreType.DEFAULT.getStoreType();

public boolean isEnableRocksDBStore() {
    return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.storeType);
}
```

这说明：

- 不是 `ROCKSDB`。
- 也不是把 tiered store 当作并列的 `storeType` 枚举值。
- tiered store 是另一套集成点，不是这里的 `storeType` 枚举值。

## 第二处容易误判：RocksDBMessageStore 并没有推翻 DefaultMessageStore

最关键的源码只有几行：

```java
public class RocksDBMessageStore extends DefaultMessageStore {
    @Override
    public ConsumeQueueStoreInterface createConsumeQueueStore() {
        return new RocksDBConsumeQueueStore(this);
    }
}
```

Broker 初始化时只是做实例切换：

```java
if (this.messageStoreConfig.isEnableRocksDBStore()) {
    defaultMessageStore = new RocksDBMessageStore(...);
} else {
    defaultMessageStore = new DefaultMessageStore(...);
}
```

这意味着：

- `CommitLog` 主骨架仍然来自 `DefaultMessageStore`。
- RocksDB 模式首先替换的是 ConsumeQueueStore。

## DefaultMessageStore 里本来就有 RocksDB 子系统

`DefaultMessageStore` 自己就创建了 RocksDB 相关对象：

```java
this.consumeQueueStore = createConsumeQueueStore();
this.messageRocksDBStorage = new MessageRocksDBStorage(getMessageStoreConfig());
```

在 `load()` 阶段还会把一些 RocksDB 组件注册为 commitlog dispatch store：

```java
if (messageStoreConfig.isIndexRocksDBEnable()) {
    registerCommitLogDispatchStore(this.indexRocksDBStore);
}
if (messageStoreConfig.isTransRocksDBEnable() && transMessageRocksDBStore != null) {
    registerCommitLogDispatchStore(this.transMessageRocksDBStore);
}
```

研究上更准确的说法应该是：

- `RocksDBMessageStore` 是 `DefaultMessageStore` 的特化。
- RocksDB 是“按子系统接入”，不是“整仓另起一套完全独立实现”。

## ConsumeQueue 的 RocksDB 化是最核心的一层

`RocksDBConsumeQueueStore` 自己的注释写得很直白：

```java
/**
 * we use two tables with different ColumnFamilyHandle
 * 1. RocksDBConsumeQueueTable stores CqUnit
 * 2. RocksDBConsumeQueueOffsetTable stores topic-queueId offset info
 */
```

也就是两张表：

- `RocksDBConsumeQueueTable`：存真正的 CQ 单元。
- `RocksDBConsumeQueueOffsetTable`：存 topic-queue 维度的最小/最大 offset 与物理 offset。

这比“单个 KV 表替代文件”更准确。

## MessageRocksDBStorage 承载的是共享 RocksDB 基座

`MessageRocksDBStorage` 里能直接看到几个列族：

```java
public static final byte[] TIMER_COLUMN_FAMILY = "timer".getBytes(...);
public static final byte[] TRANS_COLUMN_FAMILY = "trans".getBytes(...);
private static final String ROCKSDB_MESSAGE_DIRECTORY = "rocksdbstore";
```

它不是主 CommitLog 的替代物，而更像：

- timer 数据的 RocksDB 基座
- transaction 状态的 RocksDB 基座
- 部分索引 / 检查点的 RocksDB 基座

## 事务与定时消息怎么接进来

事务状态的 RocksDB 路径在 `TransMessageRocksDBStore`：

```java
public class TransMessageRocksDBStore implements CommitLogDispatchStore
```

它会在 dispatch 阶段把 half/op 事务消息转换成 `TransRocksDBRecord`。

timer 侧则由 `TimerMessageStore` 和 `MessageRocksDBStorage` 配合，源码里还能看到：

```java
LOGGER.info("now timer use rocksdb to driver, so will not enqueue in timer wheel");
```

所以更准确的理解是：

- 定时消息不是“完全抛弃 TimerLog”这么简单。
- 5.5.0 的重点是让 timer / trans / index / CQ 逐步有 RocksDB 版本。

## 研究时要避免的三个错误说法

- “RocksDB 模式把 CommitLog 也放进 RocksDB 了。”
- “storeType 直接填 `ROCKSDB` 或另一个并列引擎值就行。”
- “RocksDBConsumeQueue 只有一张表。”

这些说法和 5.5.0 源码都对不上。

## 建议一起看

- [[41-延迟消息Store层]]