# Pop 消费模式详解 — 服务端调度的核心实现

> 📄 源码路径：`broker/src/main/java/org/apache/rocketmq/broker/pop/`
> 🏷️ #核心类 #面试高频

## 🎯 Pop 消费的本质

Pop 消费是 RocketMQ 5.x 引入的全新消费模型，核心思想：
**消费者不再绑定 Queue，而是向 Broker "弹出"（Pop）消息。Broker 负责调度和状态管理。**

## 📐 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Pop 消费架构                                  │
│                                                                     │
│  Consumer                                                           │
│     │                                                               │
│     │ pop(groupId, topic, batchSize, invisibleTime)                 │
│     ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ PopConsumerService (Broker 端)                                │   │
│  │                                                              │   │
│  │  ① 获取消费 offset (getPopOffset)                             │   │
│  │  ② 从 Retry Topic 读取待重试消息                              │   │
│  │  ③ 从 Normal Topic 读取新消息                                 │   │
│  │  ④ 写入 PopConsumerRecord（记录到 KVStore/Cache）              │   │
│  │  ⑤ 返回消息 + PopCheckPoint (receipt handle)                  │   │
│  └──────────────────────────┬───────────────────────────────────┘   │
│                             │                                       │
│     ┌───────────────────────┼───────────────────────┐               │
│     ▼                       ▼                       ▼               │
│  ┌──────────┐        ┌──────────────┐       ┌──────────────┐       │
│  │ ACK      │        │ Change       │       │ Revive       │       │
│  │ (确认)    │        │ InvisibleTime│       │ (超时重试)    │       │
│  │          │        │ (续期)        │       │              │       │
│  │ 删除记录  │        │ 更新时间      │       │ 扫描过期记录  │       │
│  │          │        │              │       │ 写入 Retry   │       │
│  └──────────┘        └──────────────┘       └──────────────┘       │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ PopConsumerCache / PopConsumerKVStore (RocksDB)               │   │
│  │ 存储 PopConsumerRecord：谁在什么时候 Pop 了哪条消息           │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## 🔍 核心流程详解

### 1. Pop 消息 — `popAsync()`

```java
public CompletableFuture<PopConsumerContext> popAsync(
    String clientHost, long popTime, long invisibleTime,
    String groupId, String topicId, int queueId, int batchSize,
    boolean fifo, String attemptId, int initMode, MessageFilter filter) {

    PopConsumerContext context = new PopConsumerContext(...);

    // ① 尝试获取锁（防止同一 group@topic 并发 pop）
    if (!consumerLockService.tryLock(groupId, topicId)) {
        return CompletableFuture.completedFuture(context);
    }

    // ② 计算是否优先从 Retry Topic 消费
    boolean preferRetry = probability > 0 && requestCount % (100 / probability) == 0L;

    // ③ 构建消息获取链
    CompletableFuture<PopConsumerContext> getMessageFuture = ...;

    if (!fifo && preferRetry) {
        // 先从 Retry Topic 获取（重试消息优先）
        getMessageFuture = getMessageFromTopicAsync(..., retryTopicV1, ...);
        getMessageFuture = getMessageFromTopicAsync(..., retryTopicV2, ...);
    }

    // 再从 Normal Topic 获取
    getMessageFuture = getMessageFromTopicAsync(..., topicId, ...);

    if (!fifo && !preferRetry) {
        // 最后从 Retry Topic 获取（兜底）
        getMessageFuture = getMessageFromTopicAsync(..., retryTopicV1, ...);
        getMessageFuture = getMessageFromTopicAsync(..., retryTopicV2, ...);
    }

    // ④ 写入 PopConsumerRecord
    return getMessageFuture.thenCompose(result -> {
        if (result.isFound() && !result.isFifo()) {
            if (enablePopBufferMerge && !popConsumerCache.isCacheFull()) {
                popConsumerCache.writeRecords(result.getPopConsumerRecordList());
            } else {
                popConsumerStore.writeRecords(result.getPopConsumerRecordList());
            }
        }
        return CompletableFuture.completedFuture(result);
    });
}
```

> [!important] 消息获取顺序
> Pop 消费不是简单的"从 Queue 拉消息"，而是一个**多源合并**的过程：
>
> ```
> 1. Retry Topic V1（重试消息，旧版格式）
> 2. Retry Topic V2（重试消息，新版格式）
> 3. Normal Topic（正常消息）
> 4. 再次检查 Retry Topic（兜底）
> ```
>
> **为什么优先消费 Retry？**
> 重试消息已经失败过一次，越早重试越好，避免堆积。

### 2. PopCheckPoint — 消息收据

```java
public class PopCheckPoint {
    private long startOffset;      // 起始 offset（在 Queue 中的位置）
    private long popTime;          // Pop 时间戳
    private long invisibleTime;    // 不可见时长（超时后可重新投递）
    private int bitMap;            // 位图：标记哪些消息已被 ack
    private byte num;              // 本批次消息数量
    private int queueId;           // Queue ID
    private String topic;          // Topic
    private String cid;            // Consumer Group ID
    private long reviveOffset;     // 在 Revive Topic 中的 offset
    private List<Integer> queueOffsetDiff;  // offset 差值列表（支持非连续消息）
    private String brokerName;     // Broker 名称
    private String rePutTimes;     // 重新投递次数
    private boolean suspend;       // nack 时不增加重试次数
}
```

> [!tip] PopCheckPoint 的作用
> Consumer 收到消息时，同时收到一个 PopCheckPoint（编码在 `PROPERTY_POP_CK` 属性中）。
> 后续 ack/changeInvisibleDuration 时需要带上这个 checkpoint，Broker 用它来定位消息。

### 3. ACK — 消息确认

```java
public CompletableFuture<Boolean> ackAsync(
    long popTime, long invisibleTime, String groupId,
    String topicId, int queueId, long offset) {

    // 构建 PopConsumerRecord
    PopConsumerRecord record = new PopConsumerRecord(
        popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null);

    // 从 Cache 或 KVStore 中删除记录
    if (enablePopBufferMerge && popConsumerCache != null) {
        if (popConsumerCache.deleteRecords(Collections.singletonList(record)).isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
    }
    this.popConsumerStore.deleteRecords(Collections.singletonList(record));
    return CompletableFuture.completedFuture(true);
}
```

> [!note] ACK 的本质
> ACK = 从 PopConsumerRecord 存储中**删除记录**。
> 如果记录被删除了，Revive 服务扫描时就不会找到它，消息就不会被重试。

### 4. ChangeInvisibleDuration — 续期

```java
public void changeInvisibilityDuration(
    long popTime, long invisibleTime, long changedPopTime, long changedInvisibleTime,
    String groupId, String topicId, int queueId, long offset, boolean suspend) {

    // ① 写入新的 checkpoint（新的 popTime + invisibleTime）
    PopConsumerRecord ckRecord = new PopConsumerRecord(
        changedPopTime, groupId, topicId, queueId, 0, changedInvisibleTime, offset, null, suspend);
    this.popConsumerStore.writeRecords(Collections.singletonList(ckRecord));

    // ② 删除旧的 checkpoint
    PopConsumerRecord ackRecord = new PopConsumerRecord(
        popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null, suspend);
    this.popConsumerStore.deleteRecords(Collections.singletonList(ackRecord));
}
```

> [!tip] 续期 = 删除旧记录 + 写入新记录
> 相当于把消息的"超时时间"推迟了。新记录有新的 `popTime + invisibleTime`，
> Revive 服务会在新的超时时间后扫描到它。

### 5. Revive — 超时重试（核心！）

```java
// Revive 服务定期扫描
public long revive(AtomicLong currentTime, int maxCount) {
    long upperTime = System.currentTimeMillis() - 50L;

    // ① 扫描过期的 PopConsumerRecord
    List<PopConsumerRecord> consumerRecords = this.popConsumerStore.scanExpiredRecords(
        currentTime.get() - TimeUnit.SECONDS.toMillis(3), upperTime, maxCount);

    // ② 逐条处理
    for (PopConsumerRecord record : consumerRecords) {
        future = this.revive(record);  // 处理单条
    }

    // ③ 处理失败的记录（指数退避重试）
    this.popConsumerStore.writeRecords(new ArrayList<>(failureList));

    // ④ 删除已处理的记录
    this.popConsumerStore.deleteRecords(consumerRecords);
}
```

```java
// 处理单条记录
public CompletableFuture<Boolean> revive(PopConsumerRecord record) {
    return this.getMessageAsync(record)        // ① 根据 offset 读取原始消息
        .thenCompose(result -> {
            if (result.getLeft() == null) {
                // 消息不存在了（已 ack 或已过期），不需要重试
                return CompletableFuture.completedFuture(true);
            }
            // ② 写入 Retry Topic
            return CompletableFuture.completedFuture(this.reviveRetry(record, result.getLeft()));
        });
}
```

```java
// 写入重试队列
public boolean reviveRetry(PopConsumerRecord record, MessageExt messageExt) {
    String retryTopic = KeyBuilder.buildPopRetryTopic(topicId, groupId);
    this.createRetryTopicIfNeeded(groupId, retryTopic);

    // 构建重试消息
    MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
    msgInner.setTopic(retryTopic);
    msgInner.setBody(messageExt.getBody());
    msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);  // 重试次数 +1

    // 设置首次 Pop 时间（用于延迟级别计算）
    msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(record.getPopTime()));

    // 写入 Retry Topic
    PutMessageResult putMessageResult =
        brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
    return putMessageResult.getAppendMessageResult().getStatus() == AppendMessageStatus.PUT_OK;
}
```

## 📐 Pop 消费完整生命周期

```
时间线 ─────────────────────────────────────────────────────────────→

① Consumer 发送 Pop 请求
   │
   ▼
② Broker 从 ConsumeQueue 读取消息
   │
   ▼
③ Broker 写入 PopConsumerRecord (RocksDB/Cache)
   │  记录：(popTime, groupId, topicId, queueId, offset, invisibleTime)
   │
   ▼
④ Broker 返回消息 + PopCheckPoint 给 Consumer
   │
   ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ 分支 A：Consumer ACK（正常消费成功）                           │
   │   │                                                          │
   │   ▼                                                          │
   │ Broker 删除 PopConsumerRecord                                │
   │   → Revive 服务扫描时找不到此记录                              │
   │   → 消息生命周期结束 ✓                                        │
   └──────────────────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────────────────────┐
   │ 分支 B：Consumer 续期（消费中，需要更多时间）                    │
   │   │                                                          │
   │   ▼                                                          │
   │ Broker 删除旧记录 + 写入新记录（新 popTime + 新 invisibleTime）│
   │   → Revive 服务在新的超时时间后扫描                            │
   └──────────────────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────────────────────┐
   │ 分支 C：Consumer 超时未 ACK（消费失败或客户端宕机）             │
   │   │                                                          │
   │   ▼                                                          │
   │ Revive 服务扫描到过期记录                                     │
   │   │                                                          │
   │   ├─ 读取原始消息                                             │
   │   ├─ 构建重试消息（reconsumeTimes + 1）                       │
   │   └─ 写入 Retry Topic (%RETRY%groupId)                       │
   │       → 下次 Pop 时会优先消费 Retry Topic 中的消息             │
   └──────────────────────────────────────────────────────────────┘
```

## 📐 FIFO 顺序消费

```java
// FIFO 消息的特殊处理
public void setFifoBlocked(PopConsumerContext context,
    String groupId, String topicId, int queueId,
    List<Long> queueOffsetList, GetMessageResult getMessageResult) {

    // 更新 ConsumerOrderInfo（记录当前正在消费的 offset 列表）
    brokerController.getConsumerOrderInfoManager().update(
        context.getAttemptId(), false, topicId, groupId, queueId,
        context.getPopTime(), context.getInvisibleTime(),
        queueOffsetList, context.getOrderCountInfoBuilder(), getMessageResult);
}

// 检查是否被阻塞（前一条消息还没 ack）
public boolean isFifoBlocked(PopConsumerContext context,
    String groupId, String topicId, int queueId) {
    return brokerController.getConsumerOrderInfoManager().checkBlock(
        context.getAttemptId(), topicId, groupId, queueId, context.getInvisibleTime());
}
```

> [!important] FIFO 消费的阻塞机制
> ```
> Queue: [Msg0] [Msg1] [Msg2] [Msg3]
>              ↑
>         Consumer 正在处理 Msg1（未 ack）
>
> 此时 Pop Msg2 → isFifoBlocked() = true → 不返回 Msg2
> 等 Msg1 ACK 后 → isFifoBlocked() = false → 才返回 Msg2
> ```
>
> 保证同一 Queue 内的消息**严格顺序消费**。

## 📐 PopConsumerCache — 内存缓冲

```java
public class PopConsumerCache extends ServiceThread {
    // key = groupId@topicId@queueId
    private final ConcurrentMap<String, ConsumerRecords> consumerRecordTable;
    private final AtomicInteger estimateCacheSize;
}
```

> [!tip] Cache vs KVStore
> | | PopConsumerCache | PopConsumerKVStore (RocksDB) |
> |---|---|---|
> | 存储 | 内存 ConcurrentHashMap | 磁盘 RocksDB |
> | 速度 | 极快 | 较慢 |
> | 容量 | 有限（`popCkMaxBufferSize`） | 无限 |
> | 持久化 | 否（重启丢失） | 是 |
> | 适用 | 高吞吐场景 | 默认 |
>
> Cache 模式下，Revive 服务从内存扫描，性能更高。
> 但 Cache 容量有限，满了就降级到 RocksDB。

## 📊 核心数据结构

### PopConsumerRecord

```java
public class PopConsumerRecord {
    private long popTime;        // Pop 时间戳
    private String groupId;      // 消费组
    private String topicId;      // Topic
    private int queueId;         // Queue ID
    private int retryFlag;       // 重试标志
    private long invisibleTime;  // 不可见时长
    private long offset;         // 消息在 Queue 中的 offset
    private int attemptTimes;    // 重试尝试次数
    private String attemptId;    // 尝试 ID
    private boolean suspend;     // nack 时不增加重试次数
}
```

### AckMsg

```java
public class AckMsg {
    private long ackOffset;      // 要确认的 offset
    private long startOffset;    // PopCheckPoint 的起始 offset
    private String consumerGroup;
    private String topic;
    private int queueId;
    private long popTime;        // Pop 时间戳（用于定位记录）
    private String brokerName;
}
```

## 💡 设计亮点

### 1. 超时重试而非即时重试

```
传统 Push 模式：
  消费失败 → 立即重试（可能频繁失败）

Pop 模式：
  消费失败 → 等 invisibleTime 超时 → Revive 服务扫描 → 写入 Retry Topic
  → 下次 Pop 时优先消费重试消息

好处：
  - 不会频繁重试失败消息
  - 重试间隔可配置
  - 失败消息不会阻塞正常消息
```

### 2. 指数退避重试

```java
// Revive 失败后的退避间隔
static final int[] REWRITE_INTERVALS_IN_SECONDS =
    new int[] {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

long backoffInterval = 1000L * REWRITE_INTERVALS_IN_SECONDS[
    Math.min(REWRITE_INTERVALS_IN_SECONDS.length - 1, record.getAttemptTimes())];
```

```
重试间隔：10s → 30s → 60s → 120s → ... → 7200s（2小时）
```

### 3. 位图标记已 ACK 消息

```
PopCheckPoint.bitMap：

假设一批 Pop 了 8 条消息：
  [Msg0] [Msg1] [Msg2] [Msg3] [Msg4] [Msg5] [Msg6] [Msg7]

ACK Msg2 后：
  bitMap = 0b00000100  (第 2 位被置位)

ACK Msg5 后：
  bitMap = 0b00100100  (第 2、5 位被置位)

Revive 服务扫描时：
  只重试 bitMap 中未被置位的消息（Msg0,1,3,4,6,7）
```

### 4. 无锁设计

```
Pop 消费不需要维护 Consumer 和 Queue 的绑定关系：
  - 4.x Push：Rebalance → 分配 Queue → 绑定 Consumer
  - 5.x Pop：直接 Pop，Broker 调度

好处：
  - Consumer 无状态，随时扩缩容
  - 不需要 Rebalance（最复杂的部分）
  - Broker 端统一调度，更可控
```

## 🧩 Pop 消费 vs Push 消费对比

| 维度 | 4.x Push 消费 | 5.x Pop 消费 |
|------|-------------|-------------|
| 调度方 | Consumer（客户端） | Broker（服务端） |
| Queue 绑定 | 需要 Rebalance | 不需要 |
| 消费进度 | Consumer 本地管理 | Broker 端管理（PopConsumerRecord） |
| 重试机制 | 本地重试队列 | Revive 服务 + Retry Topic |
| 顺序消费 | Queue 级别锁定 | ConsumerOrderInfoManager |
| 扩缩容 | 需要 Rebalance | 直接扩缩容 |
| 客户端复杂度 | 高 | 低 |

---

## 💡 学习收获

1. **PopCheckPoint**：消息收据，用位图标记 ack 状态
2. **Revive 服务**：超时重试的核心，扫描过期记录写入 Retry Topic
3. **多源合并**：Pop 时同时检查 Normal + Retry Topic
4. **指数退避**：失败重试间隔递增，避免频繁失败
5. **FIFO 阻塞**：通过 ConsumerOrderInfoManager 保证顺序
6. **Cache + KVStore**：双层存储，兼顾性能和持久化
