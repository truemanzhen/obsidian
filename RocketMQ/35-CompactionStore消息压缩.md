# CompactionStore 消息压缩存储

> 源码位置：`store/src/main/java/org/apache/rocketmq/store/kv/`

## 🏗️ 什么是 Compaction

Compaction（压缩）是一种**保留每个 Key 最新值**的存储策略，类似于 Kafka 的 Log Compaction。

**典型场景：**
- 状态快照（每个 Key 只需要最新状态）
- 配置变更（只需要最新配置）
- 本地缓存同步（只需要最新数据）

```
原始消息流：
Key1:v1 → Key2:v1 → Key1:v2 → Key3:v1 → Key2:v2 → Key1:v3

Compaction 后：
Key1:v3 → Key3:v1 → Key2:v2
（只保留每个 Key 的最新值）
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                    CompactionStore                        │
├──────────────┬──────────────┬────────────────────────────┤
│ CompactionLog │ Compaction   │ Compaction                 │
│ (数据文件)    │ PositionMgr  │ CQ (Compaction后的CQ)      │
│              │ (进度管理)    │                            │
├──────────────┴──────────────┴────────────────────────────┤
│                  CompactionService                        │
│            (CommitLog → CompactionLog 的桥梁)              │
├─────────────────────────────────────────────────────────┤
│              CommitLogDispatcherCompaction                 │
│            (从 CommitLog 分发到 CompactionStore)           │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `CompactionStore` | 压缩存储入口，管理所有 CompactionLog |
| `CompactionLog` | 单个 Topic 的压缩日志文件 |
| `CompactionService` | 服务层，处理 Dispatch 请求 |
| `CompactionPositionMgr` | 进度管理，记录每个 Topic 的处理位置 |
| `CompactionLog` | 底层文件操作（读写压缩数据） |
| `CommitLogDispatcherCompaction` | CommitLog 分发器，触发压缩 |

## 📤 写入流程

```
消息写入 CommitLog
    ↓
CommitLogDispatcherCompaction.dispatch(request)
    ├── 检查 Topic 的 CleanupPolicy 是否为 COMPACTION
    ├── 读取消息体
    └── 调用 CompactionService.putRequest(request)

CompactionService.putRequest(request)
    ├── 检查 CleanupPolicy == COMPACTION
    ├── 从 CommitLog 读取消息数据
    └── CompactionStore.doDispatch(request, msgData)

CompactionStore.doDispatch(request, msgData)
    ├── 获取或创建 CompactionLog（按 Topic）
    ├── 提取消息 Key
    └── CompactionLog.put(key, value, timestamp)
        ├── 写入 CompactionLog 文件
        └── 更新内存索引
```

## 🔄 Compaction 过程

### 定时触发

```java
// CompactionStore 后台定时任务
compactionSchedule.scheduleWithFixedDelay(() -> {
    try {
        doCompaction();
    } catch (Exception e) {
        log.error("Compaction failed", e);
    }
}, scanInterval, compactionInterval, TimeUnit.MILLISECONDS);
```

### Compaction 算法

```
1. 遍历 CompactionLog 中的所有 Key
2. 对每个 Key，保留最新的值（按 timestamp 排序）
3. 生成新的 CompactionLog 文件
4. 替换旧文件
5. 更新 PositionMgr
```

### OffsetMap 设计

```java
// 使用 OffsetMap 去重
// Key hash → 最新 offset
private final LongHashSet offsetMap;

// 遍历时：
for (long offset = startOffset; offset < endOffset; offset++) {
    byte[] key = readKey(offset);
    long keyHash = hash(key);
    // 如果已存在且 offset 更大，跳过
    if (offsetMap.contains(keyHash) && existingOffset > offset) {
        continue; // 旧数据，跳过
    }
    offsetMap.put(keyHash, offset);
}
```

## 📂 文件结构

```
${storePath}/compaction/
├── compactionLog/              # 压缩后的数据文件
│   ├── TopicA/
│   │   ├── 00000000000000000000  # CompactionLog 文件
│   │   └── 00000000000000100000
│   └── TopicB/
│       └── 00000000000000000000
├── compactionCq/              # Compaction 后的 ConsumeQueue
│   ├── TopicA/
│   └── TopicB/
└── compactionPosition.json    # 进度信息
```

## 📊 数据格式

### CompactionLog 条目

```
┌──────────────────────────────────────────┐
│           CompactionLog Entry            │
├──────────┬───────────┬───────────────────┤
│ 4 bytes  │ key length│                   │
│ N bytes  │ key data  │  消息 Key         │
│ 4 bytes  │ value len │                   │
│ M bytes  │ value data│  消息体           │
│ 8 bytes  │ timestamp │  消息时间戳       │
└──────────┴───────────┴───────────────────┘
```

## 🔗 与 CleanupPolicy 的关系

```java
// Topic 配置中指定清理策略
public enum CleanupPolicy {
    DELETE,      // 默认：按时间/大小删除
    COMPACTION;  // 压缩：保留每个 Key 最新值
}

// 创建 Topic 时指定
TopicConfig config = new TopicConfig();
config.setCleanupPolicy(CleanupPolicy.COMPACTION);
```

## 💡 设计亮点

1. **懒加载**：CompactionLog 按需创建
2. **增量 Compaction**：只处理新增数据，不全量重写
3. **OffsetMap 去重**：高效判断 Key 是否已存在
4. **多线程 Compaction**：`compactionThreadNum` 可配置
5. **进度持久化**：PositionMgr 记录每个 Topic 的处理位置

## ⚠️ 注意事项

1. **Key 必须存在**：Compaction Topic 的消息必须有 Key
2. **磁盘空间**：Compaction 前后需要双倍空间
3. **Compaction 频率**：过于频繁会增加 I/O，过于稀少会占用空间
4. **顺序消费**：Compaction 后消息顺序可能改变

## 🔗 与其他模块的关系

- **CommitLog**：CompactionStore 从 CommitLog 读取原始消息
- **ConsumeQueue**：Compaction 后生成独立的 CQ
- **CleanupPolicy**：Topic 配置决定是否启用 Compaction
- **TieredStore**：Compaction 后的数据适合分层存储
- **CommitLogDispatcher**：分发器触发 CompactionService

---

#核心类 #源码技巧
