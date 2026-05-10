# 延迟消息 Store 层

> 源码位置：`store/src/main/java/org/apache/rocketmq/store/timer/`

## 🏗️ 整体架构

延迟消息在 Store 层由 `TimerMessageStore` 管理，负责消息的**定时投递**和**持久化**。

```
┌─────────────────────────────────────────────────────────┐
│                   TimerMessageStore                      │
├─────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────┐ │
│  │                TimerWheel                           │ │
│  │  ┌────────────────────────────────────────────┐   │ │
│  │  │ Slot-0 │ Slot-1 │ Slot-2 │ ... │ Slot-N   │   │ │
│  │  │(10ms)  │(20ms)  │(30ms)  │     │(N*10ms) │   │ │
│  │  └────────────────────────────────────────────┘   │ │
│  └────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────┐ │
│  │              TimerLog                               │ │
│  │  消息持久化存储（二进制格式）                         │ │
│  └────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────┐ │
│  │           Enqueue / Dequeue 线程                    │ │
│  │  Enqueue: 消息 → TimerWheel → TimerLog             │ │
│  │  Dequeue: TimerLog → 原 Topic → 正常消费            │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `TimerMessageStore` | 延迟消息存储入口 |
| `TimerWheel` | 时间轮（内存结构） |
| `TimerLog` | 延迟消息持久化日志 |
| `TimerCheckpoint` | 检查点（持久化进度） |
| `TimerMetrics` | 延迟消息指标 |
| `TimerRequest` | 延迟消息请求 |
| `Slot` | 时间轮槽位 |

## ⏰ TimerWheel（时间轮）

### 数据结构

```java
public class TimerWheel {
    // 每个 Slot 的时间间隔（默认 10ms）
    private final int slotDurationMs;

    // Slot 数量
    private final int slotNum;

    // 时间轮数组
    private final Slot[] slots;

    // 当前时间指针
    private volatile long currentTimeMs;
}
```

### Slot 结构

```java
public class Slot {
    // 延迟消息列表（按 offset 排序）
    private final TreeMap<Long, TimerRequest> requests;

    // Slot 时间戳
    private long timeMs;

    // 是否已过期
    private boolean expired;
}
```

### 工作原理

```
时间轮示例（slotDurationMs=10ms, slotNum=3600）：
┌─────────────────────────────────────────────────────────┐
│  Slot-0   │ Slot-1   │ Slot-2   │ ... │ Slot-3599     │
│  (0-10ms) │(10-20ms) │(20-30ms) │     │(35990-36000ms)│
│  [msg1]   │ [msg2]   │ [msg3]   │     │ [msgN]        │
└─────────────────────────────────────────────────────────┘
         ↑
    currentTimeMs（每 10ms 移动一格）

总覆盖时间 = slotDurationMs × slotNum = 10ms × 3600 = 36s
（可配置更大的 slotNum 覆盖更长时间）
```

## 📝 TimerLog（持久化日志）

### 文件格式

```
┌─────────────────────────────────────────────────────────┐
│                   TimerLog 文件                           │
├─────────────────────────────────────────────────────────┤
│  FileHeader                                             │
│  ├── magicNumber (4 bytes)                              │
│  ├── version (4 bytes)                                  │
│  └── slotCount (4 bytes)                                │
├─────────────────────────────────────────────────────────┤
│  Slot 区域                                              │
│  ├── Slot-0: offset + size + timestamp                  │
│  ├── Slot-1: offset + size + timestamp                  │
│  └── ...                                                │
├─────────────────────────────────────────────────────────┤
│  数据区域                                               │
│  ├── TimerRequest-0 (序列化的消息)                       │
│  ├── TimerRequest-1 (序列化的消息)                       │
│  └── ...                                                │
└─────────────────────────────────────────────────────────┘
```

### TimerRequest 格式

```java
public class TimerRequest {
    private long offset;           // CommitLog 中的 offset
    private int size;              // 消息大小
    private long deliverTimeMs;    // 投递时间戳
    private int queueId;           // 目标队列 ID
    private String topic;          // 目标 Topic
    private long storeTime;        // 存储时间
}
```

## 📤 Enqueue 流程（消息入队）

```
消息到达（delayLevel > 0）
    ↓
TimerMessageStore.putMessage(msg)
    ├── 计算投递时间 = 当前时间 + 延迟时间
    ├── 构建 TimerRequest
    │   ├── offset = CommitLog offset
    │   ├── deliverTimeMs = 当前时间 + delayTime
    │   └── topic = 原始 Topic
    ├── 写入 TimerLog
    │   ├── 序列化 TimerRequest
    │   └── 写入文件
    ├── 加入 TimerWheel
    │   ├── 计算 Slot = (deliverTimeMs - currentTimeMs) / slotDurationMs
    │   └── 加入对应 Slot
    └── 更新 TimerCheckpoint
```

## 📥 Dequeue 流程（消息出队）

```
TimerMessageStore 后台线程（每 10ms）
    ↓
扫描当前 Slot
    ├── 获取所有到期的 TimerRequest
    ├── 从 CommitLog 读取原始消息
    ├── 重建消息（修改 Topic 为原始 Topic）
    ├── 写入原始 Topic 的 ConsumeQueue
    ├── 从 TimerLog 中删除
    └── 更新 TimerCheckpoint
```

### 到期判断

```java
// 检查 Slot 是否到期
private boolean isSlotExpired(Slot slot, long currentTimeMs) {
    return slot.getTimeMs() <= currentTimeMs;
}
```

## 📊 TimerCheckpoint（检查点）

```java
public class TimerCheckpoint {
    private long timerLogOffset;        // TimerLog 处理进度
    private long timerLogSize;          // TimerLog 当前大小
    private long currentTimeMs;         // 当前时间指针
    private long lastCheckpointTimeMs;  // 上次检查点时间
}
```

## 🔧 RocksDB 版本（5.5.0）

5.5.0 提供了基于 RocksDB 的实现：

```java
public class TimerMessageRocksDBStore {
    // 使用 RocksDB 存储延迟消息
    // Key: deliverTimeMs + topic + queueId
    // Value: TimerRequest 序列化数据

    // 优势：
    // - 不需要固定的 slotNum
    // - 支持更长的延迟时间
    // - 更好的随机读写性能
}
```

## 💡 设计亮点

1. **时间轮算法**：O(1) 入队，O(1) 出队，高效
2. **持久化保障**：TimerLog 持久化延迟消息，崩溃可恢复
3. **检查点机制**：TimerCheckpoint 记录进度，避免重复投递
4. **RocksDB 升级**：5.5.0 提供 RocksDB 版本，更灵活
5. **分离存储**：TimerLog 和 CommitLog 分离，互不影响

## ⚠️ 注意事项

1. **延迟精度**：slotDurationMs 决定精度（默认 10ms）
2. **最大延迟**：slotDurationMs × slotNum = 最大延迟时间
3. **内存占用**：每个 Slot 在内存中维护消息列表
4. **磁盘空间**：TimerLog 会定期清理已投递的消息
5. **重启恢复**：重启时需要从 TimerLog 和 Checkpoint 恢复

## 🔗 与其他模块的关系

- **CommitLog**：消息先写入 CommitLog，再入队到 TimerLog
- **ConsumeQueue**：到期消息写入原始 Topic 的 ConsumeQueue
- **延迟消息笔记**：`21-延迟消息设计` 笔记覆盖了 Proxy 层，本文补充 Store 层
- **RocksDB**：`TimerMessageRocksDBStore` 是 RocksDB 版本实现

---

#核心类 #源码技巧
