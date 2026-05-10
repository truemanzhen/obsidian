# Offset 管理机制

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/offset/`

## 🏗️ 整体架构

Offset 管理是消息可靠性的核心——消费者消费到哪里了，Broker 必须精确记录。

```
┌──────────────────────────────────────────────────────────┐
│                    Offset 管理体系                        │
├──────────────┬──────────────┬────────────────────────────┤
│ ConsumerOffset│ BroadcastOffset│ LmqConsumerOffset       │
│ Manager      │ Manager        │ Manager                 │
│ (集群模式)    │ (广播模式)     │ (LMQ 独立Offset)        │
├──────────────┴──────────────┴────────────────────────────┤
│              ConfigManager (持久化基类)                    │
│         定期 flush 到文件 / 从文件 load                    │
└──────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `ConsumerOffsetManager` | **集群模式** Offset 管理，Broker 侧持久化 |
| `BroadcastOffsetManager` | **广播模式** Offset 管理，每个客户端独立消费进度 |
| `BroadcastOffsetStore` | 广播模式 Offset 持久化（本地文件） |
| `LmqConsumerOffsetManager` | LMQ（轻量级消息队列）独立 Offset 管理 |

## 📊 ConsumerOffsetManager（集群模式）

### 数据结构

```java
public class ConsumerOffsetManager extends ConfigManager {
    // 核心数据结构：topic@group → {queueId → offset}
    protected ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable;

    // 重置后的 Offset（用于 ResetOffset 场景）
    protected final ConcurrentMap<String, ConcurrentMap<Integer, Long>> resetOffsetTable;

    // Pull 模式专用 Offset（独立于消费 Offset）
    private final ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> pullOffsetTable;
}
```

**Key 设计：`topic@group`**

```
"TopicTest@ConsumerGroupA" → {
    0 → 12345,   // queue 0 消费到 offset 12345
    1 → 67890,   // queue 1 消费到 offset 67890
    2 → 11111    // queue 2 消费到 offset 11111
}
```

### 核心操作

**1. 查询 Offset**

```java
public long queryOffset(String group, String topic, int queueId) {
    String key = topic + TOPIC_GROUP_SEPARATOR + group;
    ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
    if (map != null) {
        Long offset = map.get(queueId);
        if (offset >= 0) {
            return offset;
        }
    }
    // 查不到，返回 -1（客户端会从 max/min offset 开始消费）
    return -1;
}
```

**2. 更新 Offset**

```java
public void commitOffset(final String group, final String topic, final int queueId, final long offset) {
    String key = topic + TOPIC_GROUP_SEPARATOR + group;
    ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
    if (null == map) {
        map = new ConcurrentHashMap<>(32);
        ConcurrentMap<Integer, Long> prev = this.offsetTable.putIfAbsent(key, map);
        if (prev != null) {
            map = prev;
        }
    }
    map.put(queueId, offset);
    // 版本变更，触发持久化
    this.dataVersion.nextVersion();
}
```

**3. 批量更新（减少锁竞争）**

```java
public void commitOffset(final String group, final String topic,
    final int queueId, final long offset, final boolean increaseOnly) {
    // increaseOnly=true 时只允许递增（防止回退）
    if (increaseOnly) {
        map.compute(queueId, (k, v) -> {
            if (v == null || offset > v) {
                return offset;
            }
            return v;
        });
    } else {
        map.put(queueId, offset);
    }
}
```

### 持久化机制

```java
// 定时持久化（默认每 5 秒）
@Override
public String encode() {
    return RemotingSerializable.toJson(this);
}

@Override
    public void decode(String json) {
        ConsumerOffsetManager obj = RemotingSerializable.fromJson(json, ConsumerOffsetManager.class);
        if (obj != null) {
            this.offsetTable = obj.offsetTable;
            this.dataVersion = obj.dataVersion;
        }
    }
}
```

**持久化文件路径：** `${storePath}/config/consumerOffset.json`

### Offset 查询优先级

当客户端查询 Offset 时，Broker 的处理顺序：

```
1. 查询 resetOffsetTable（如果有重置的 Offset）
2. 查询 offsetTable（正常消费进度）
3. 返回 -1（客户端根据策略决定从 max 或 0 开始）
```

## 📡 BroadcastOffsetManager（广播模式）

广播模式下，每个客户端独立消费全部消息，需要独立记录 Offset。

### 设计挑战

1. **客户端数量多**：每个客户端都有独立的 Offset
2. **Proxy/Broker 切换**：支持从 Proxy 切回 Broker 模式时保留进度
3. **内存压力**：大量客户端的 Offset 占用内存

### 数据结构

```java
public class BroadcastOffsetManager extends ServiceThread {
    // topic@groupId → BroadcastOffsetData
    protected final ConcurrentHashMap<String, BroadcastOffsetData> offsetStoreMap;
}

public class BroadcastOffsetData {
    String topic;
    String group;
    // clientId → BroadcastTimedOffsetStore
    ConcurrentHashMap<String, BroadcastTimedOffsetStore> clientOffsetStore;
}

public class BroadcastTimedOffsetStore {
    long timestamp;                    // 最后更新时间
    // queueId → offset
    ConcurrentHashMap<Integer, Long> offsetTable;
    boolean fromProxy;                 // 是否来自 Proxy
}
```

### 核心操作

**1. 更新 Offset**

```java
public void updateOffset(String topic, String group, int queueId,
    long offset, String clientId, boolean fromProxy) {
    BroadcastOffsetData data = offsetStoreMap.computeIfAbsent(
        buildKey(topic, group), key -> new BroadcastOffsetData(topic, group));

    data.clientOffsetStore.compute(clientId, (k, timedStore) -> {
        if (timedStore == null) {
            timedStore = new BroadcastTimedOffsetStore(fromProxy);
        }
        timedStore.timestamp = System.currentTimeMillis();
        timedStore.offsetTable.put(queueId, offset);
        return timedStore;
    });
}
```

**2. 查询 Offset**

```java
public long queryOffset(String topic, String group, int queueId, String clientId) {
    BroadcastOffsetData data = offsetStoreMap.get(buildKey(topic, group));
    if (data != null) {
        BroadcastTimedOffsetStore timedStore = data.clientOffsetStore.get(clientId);
        if (timedStore != null) {
            Long offset = timedStore.offsetTable.get(queueId);
            if (offset != null) {
                return offset;
            }
        }
    }
    return -1;
}
```

**3. 定时清理过期客户端**

```java
// 后台线程定期清理长时间未上报的客户端
@Override
public void run() {
    while (!this.isStopped()) {
        // 清理超过 brokerConfig.getBroadcastOffsetExpireSeconds() 未更新的客户端
        cleanupExpiredClients();
    }
}
```

## 🔄 Offset 更新时机

### 集群模式

```
客户端消费成功
    ↓
客户端发送 UPDATE_CONSUMER_OFFSET
    ↓
ConsumerManageProcessor.updateConsumerOffset()
    ↓
ConsumerOffsetManager.commitOffset(group, topic, queueId, offset)
    ↓
内存更新 + 标记 dirty
    ↓
定时持久化到 consumerOffset.json
```

### 广播模式

```
客户端消费成功
    ↓
客户端本地记录 Offset（BroadcastOffsetStore）
    ↓
定期上报到 Broker（BroadcastOffsetManager）
    ↓
Broker 内存记录（用于 Proxy/Broker 切换）
```

## 💡 设计亮点

1. **`topic@group` 复合键**：一个 Map 管理所有 Topic-Group 组合
2. **increaseOnly 模式**：防止消费进度回退导致重复消费
3. **resetOffsetTable**：支持 Offset 重置而不丢失原始进度
4. **pullOffsetTable**：Pull 模式和 Push 模式 Offset 独立管理
5. **BroadcastOffsetData 带时间戳**：支持客户端过期清理
6. **fromProxy 标记**：支持 Proxy/Broker 模式无缝切换

## ⚠️ 常见问题

### Q: 为什么有时候消费进度不更新？

1. 客户端没有发送 UPDATE_CONSUMER_OFFSET（检查消费逻辑）
2. Broker 端 Offset 文件损坏（检查 consumerOffset.json）
3. 广播模式下客户端 ID 变化（IP 变化导致）

### Q: 如何手动重置 Offset？

```bash
# 通过 Admin 工具
mqadmin resetOffsetByTime -t TopicTest -g ConsumerGroup -s 2024-01-01#00:00:00
```

## 🔗 与其他模块的关系

- **ConsumerManageProcessor**：处理 Offset 查询/更新请求
- **PullMessageProcessor**：拉取消息时查询起始 Offset
- **PopMessageProcessor**：Pop 模式使用独立的 Offset 管理
- **Rebalance**：客户端 Rebalance 时查询各队列的消费进度
- **Proxy**：`BroadcastOffsetManager` 支持 Proxy/Broker 切换

---

#核心类 #面试高频
