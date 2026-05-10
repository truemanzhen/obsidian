# Pop Revive 机制

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/processor/PopReviveService.java` + `PopBufferMergeService.java`

## 🏗️ 什么是 Pop Revive

Pop Revive 是 Pop 消费模式的**超时重试机制**——当消费者在 `invisibleTime` 内没有确认消息（Ack），Revive 机制会自动重新投递。

```
┌─────────────────────────────────────────────────────────┐
│                   Pop Revive 流程                        │
├─────────────────────────────────────────────────────────┤
│  1. PopMessageProcessor 投递消息                        │
│     ├── 生成 PopCheckPoint（检查点）                     │
│     ├── 写入 Revive Topic（超时重试队列）                │
│     └── 设置 invisibleTime（可见时间窗口）               │
├─────────────────────────────────────────────────────────┤
│  2. 消费者处理                                          │
│     ├── 成功 → AckMessageProcessor 删除检查点            │
│     └── 失败/超时 → 检查点保留                           │
├─────────────────────────────────────────────────────────┤
│  3. PopReviveService 扫描                               │
│     ├── 读取 Revive Topic 中的检查点                     │
│     ├── 检查是否超过 invisibleTime                       │
│     └── 超时 → 重新投递消息到原 Topic                    │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `PopReviveService` | **核心**：扫描 Revive Topic，重新投递超时消息 |
| `PopBufferMergeService` | 内存缓冲层，合并 Ack/CheckPoint 操作 |
| `PopCheckPoint` | 检查点数据结构 |
| `AckMsg` / `BatchAckMsg` | 确认消息 |
| `PopMessageProcessor` | 消息投递时写入 Revive Topic |
| `AckMessageProcessor` | 消费确认时删除检查点 |

## 📝 PopCheckPoint（检查点）

```java
public class PopCheckPoint {
    private String topic;          // 原始 Topic
    private int queueId;           // 队列 ID
    private long startOffset;      // 起始 offset
    private int num;               // 消息数量
    private long popTime;          // Pop 时间戳
    private long invisibleTime;    // 可见时间窗口
    private long reviveOffset;     // Revive Topic 中的 offset
    private String consumerGroup;  // 消费组
    private int bitMap;            // 位图（标记已 Ack 的消息）
}
```

**位图设计：**
- 每个 bit 代表一条消息
- `1` = 已 Ack，`0` = 未 Ack
- 最多支持 32 条消息（int 32 位）

## 📤 投递流程（PopMessageProcessor）

```
PopMessageProcessor.processRequest()
    ├── 获取消息
    ├── 构建 PopCheckPoint
    │   ├── topic, queueId, startOffset, num
    │   ├── popTime = 当前时间戳
    │   ├── invisibleTime = 请求参数（默认 30s）
    │   └── bitMap = 0（全部未 Ack）
    ├── 写入 Revive Topic
    │   ├── Revive Topic = RMQ_SYS_REVIVE_LOG_{clientId}
    │   └── 消息体 = PopCheckPoint 序列化
    └── 返回消息 + PopCheckPoint 给消费者
```

## 📥 确认流程（AckMessageProcessor）

```
AckMessageProcessor.processRequest()
    ├── 解析 AckMsg（topic, queueId, offset）
    ├── 查找对应的 PopCheckPoint
    │   └── 通过 PopBufferMergeService 查找
    ├── 更新 CheckPoint 的 bitMap
    │   └── 将对应 offset 的 bit 设为 1
    ├── 如果所有 bit 都为 1（全部 Ack）
    │   └── 删除 Revive Topic 中的检查点
    └── 返回确认成功
```

## 🔄 PopReviveService（超时重试）

### 核心逻辑

```java
public class PopReviveService extends ServiceThread {
    private final BrokerController brokerController;
    private final String reviveTopic;
    private long reviveOffset;
    private int queueId;

    // 检查点重写间隔（递增）
    private final int[] ckRewriteIntervalsInSeconds = new int[] {
        10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200
    };

    @Override
    public void run() {
        while (!isStopped()) {
            // 1. 读取 Revive Topic
            GetMessageResult result = getMessageFromReviveTopic();

            // 2. 解析 PopCheckPoint
            PopCheckPoint ck = parseCheckPoint(result);

            // 3. 检查是否超时
            long now = System.currentTimeMillis();
            if (now - ck.getPopTime() < ck.getInvisibleTime()) {
                // 未超时，跳过
                continue;
            }

            // 4. 检查 Ack 状态
            if (isAllAcked(ck)) {
                // 全部已 Ack，删除检查点
                continue;
            }

            // 5. 重新投递未 Ack 的消息
            reviveMessage(ck);
        }
    }
}
```

### 检查点重写策略

当检查点未超时但需要保留时，PopReviveService 会重写检查点到 Revive Topic：

```java
// 重写间隔递增：10s → 20s → 30s → 60s → ... → 7200s
// 避免频繁重写消耗资源
int rewriteInterval = ckRewriteIntervalsInSeconds[rewriteTimes];
if (now - lastRewriteTime > rewriteInterval * 1000) {
    rewriteCheckPoint(ck);
}
```

### 重新投递

```java
private void reviveMessage(PopCheckPoint ck) {
    for (int i = 0; i < ck.getNum(); i++) {
        // 检查该消息是否已 Ack
        if ((ck.getBitMap() & (1 << i)) != 0) {
            continue; // 已 Ack，跳过
        }

        // 从原 Topic 读取消息
        MessageExt msg = getMessageFromOriginalTopic(ck.getTopic(), ck.getQueueId(),
            ck.getStartOffset() + i);

        // 重新投递到原 Topic（或重试 Topic）
        MessageExtBrokerInner inner = buildReviveMessage(msg);
        brokerController.getMessageStore().putMessage(inner);
    }
}
```

## 🔄 PopBufferMergeService（内存缓冲）

### 设计目的

- 减少磁盘 I/O：Ack 操作先在内存合并
- 提高吞吐量：批量写入 Revive Topic
- 快速查找：通过 mergeKey 快速定位 CheckPoint

### 数据结构

```java
public class PopBufferMergeService extends ServiceThread {
    // mergeKey → PopCheckPointWrapper
    ConcurrentHashMap<String, PopCheckPointWrapper> buffer;

    // topic@cid@queueId → QueueWithTime<PopCheckPointWrapper>
    ConcurrentHashMap<String, QueueWithTime<PopCheckPointWrapper>> commitOffsets;
}

public class PopCheckPointWrapper {
    private PopCheckPoint ck;           // 检查点
    private long reviveOffset;          // Revive Topic 中的 offset
    private boolean acked;              // 是否全部 Ack
    private long storeTime;             // 存储时间
    private List<Integer> ackBitMap;    // Ack 位图
}
```

### mergeKey 格式

```
topic@consumerGroup@queueId@startOffset@popTime
```

### 合并流程

```
AckMessageProcessor 收到 Ack
    ↓
PopBufferMergeService.addAck(ackMsg)
    ├── 通过 mergeKey 查找 PopCheckPointWrapper
    ├── 更新 bitMap（标记已 Ack）
    ├── 如果全部 Ack
    │   ├── 从 buffer 中移除
    │   └── 删除 Revive Topic 中的检查点
    └── 否则保留（等待 Revive 检查）
```

## 💡 设计亮点

1. **位图标记**：用 int 的 bit 标记每条消息的 Ack 状态，高效
2. **递增重写间隔**：避免频繁重写，从 10s 递增到 7200s
3. **内存缓冲**：PopBufferMergeService 合并 Ack 操作，减少 I/O
4. **独立 Revive Topic**：每个 Pop 客户端有独立的 Revive 队列
5. **超时自动重试**：invisibleTime 到期自动重新投递

## ⚠️ 注意事项

1. **invisibleTime 设置**：太短会导致频繁重试，太长会延迟失败消息的重投
2. **位图限制**：int 只有 32 位，单次 Pop 最多 32 条消息
3. **Revive Topic 清理**：需要定期清理已处理的检查点
4. **重复消费**：超时重试可能导致消息重复消费（需要幂等）

## 🔗 与其他模块的关系

- **PopMessageProcessor**：投递时写入 Revive Topic
- **AckMessageProcessor**：确认时更新 CheckPoint
- **Store 层**：Revive Topic 是一个普通的系统 Topic
- **长轮询**：PopBufferMergeService 与长轮询配合

---

#核心类 #源码技巧 #面试高频
