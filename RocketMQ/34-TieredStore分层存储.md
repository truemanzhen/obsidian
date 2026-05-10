# TieredStore 分层存储

> 源码位置：`tieredstore/src/main/java/org/apache/rocketmq/tieredstore/`

## 🏗️ 为什么需要分层存储

传统 RocketMQ 所有消息都存在本地磁盘，但：
- **热数据**（最近的消息）需要高性能 SSD
- **冷数据**（历史消息）可以放在便宜的 HDD 或对象存储上
- 混合存储可以**降低成本**同时**保持性能**

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                   TieredMessageStore                     │
│              (实现 MessageStore 接口)                     │
├─────────────────────────────────────────────────────────┤
│                    核心组件                              │
│  ┌──────────────────┬──────────────────┐                │
│  │ MessageStore     │ MessageStore     │                │
│  │ Dispatcher (写)  │ Fetcher (读)     │                │
│  └──────────────────┴──────────────────┘                │
├─────────────────────────────────────────────────────────┤
│                    存储层                               │
│  ┌──────────┬──────────┬──────────────────┐            │
│  │  Hot     │  Warm    │  Cold            │            │
│  │  (SSD)   │  (HDD)   │  (S3/OSS)        │            │
│  └──────────┴──────────┴──────────────────┘            │
├─────────────────────────────────────────────────────────┤
│              MetadataStore (元数据管理)                   │
│  └── Topic/Queue/FileSegment 元信息                      │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `TieredMessageStore` | 分层存储入口，实现 `MessageStore` 接口 |
| `MessageStoreDispatcher` | **写路径**：消息分发到不同存储层 |
| `MessageStoreFetcher` | **读路径**：从合适的存储层读取消息 |
| `FileSegment` | 文件分段抽象（一个物理文件） |
| `PosixFileSegment` | 本地文件分段（SSD/HDD） |
| `MemoryFileSegment` | 内存文件分段（临时缓存） |
| `FileSegmentFactory` | 文件分段工厂 |
| `MessageStoreFilter` | 消息过滤接口 |
| `MessageStoreTopicFilter` | 基于 Topic 的过滤 |

### FileSegmentType（分段类型）

```java
public enum FileSegmentType {
    COMMIT_LOG,      // CommitLog 分段
    CONSUME_QUEUE,   // ConsumeQueue 分段
    INDEX            // 索引分段
}
```

## 📤 写路径（Dispatcher）

```
消息到达
    ↓
MessageStoreDispatcher.dispatch(topic, queueId, msg)
    ├── 判断消息应该写入哪一层
    │   ├── 最近 N 小时 → Hot Tier (SSD)
    │   ├── N-M 小时内 → Warm Tier (HDD)
    │   └── M 小时前 → Cold Tier (S3/OSS)
    ├── 写入对应 FileSegment
    ├── 更新 MetadataStore
    └── 触发数据迁移（Hot → Warm → Cold）
```

### 数据迁移策略

```
后台定时任务：
1. 扫描 Hot Tier 中超过阈值的 FileSegment
2. 将数据复制到 Warm Tier
3. 验证完整性后删除 Hot Tier 数据
4. Warm → Cold 同理
```

## 📥 读路径（Fetcher）

```
消费者请求消息
    ↓
MessageStoreFetcher.getMessage(topic, queueId, offset, maxNum)
    ├── 先查 Hot Tier（内存/SSD）
    │   └── 命中 → 直接返回
    ├── 再查 Warm Tier（HDD）
    │   └── 命中 → 异步提升到 Hot Tier + 返回
    └── 最后查 Cold Tier（S3/OSS）
        └── 异步拉取 → 写入 Warm Tier → 返回
```

### 透明访问

```java
// 对上层完全透明，和普通 MessageStore 接口一致
public class TieredMessageStore implements MessageStore {
    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId,
        long offset, int maxMsgNums, MessageFilter filter) {
        // 内部自动路由到合适的存储层
        return fetcher.getMessage(group, topic, queueId, offset, maxMsgNums, filter);
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // 内部自动分发到合适的存储层
        return dispatcher.putMessage(msg);
    }
}
```

## 🗂️ MetadataStore

管理分层存储的元数据：

```java
// Topic 元数据
TopicMetadata {
    String topic;
    int queueNum;
    long minOffset;
    long maxOffset;
}

// FileSegment 元数据
FileSegmentMetadata {
    FileSegmentType type;
    String path;
    long baseOffset;
    long size;
    long timestamp;
    StorageTier tier;  // HOT/WARM/COLD
}
```

## 💡 设计亮点

1. **接口兼容**：实现 `MessageStore` 接口，上层无感知
2. **透明迁移**：Hot → Warm → Cold 自动流转
3. **异步拉取**：Cold Tier 数据异步加载，不阻塞
4. **按 Topic 配置**：不同 Topic 可以有不同的分层策略
5. **Provider 可插拔**：通过 `FileSegmentProvider` 支持不同存储后端

## ⚠️ 注意事项

1. **网络带宽**：S3/OSS 的读写带宽是瓶颈
2. **数据一致性**：迁移过程中需要保证数据不丢失
3. **元数据管理**：MetadataStore 本身也需要高可用
4. **冷启动**：大量冷数据拉取需要预热时间

## 🔗 与其他模块的关系

- **Store 层**：`TieredMessageStore` 是 `MessageStore` 的另一种实现
- **CompactionStore**：Compaction Topic 的数据适合分层存储
- **RocksDB**：MetadataStore 底层可以用 RocksDB
- **Broker**：通过 `MessageStoreConfig.storeType=TIERED_STORAGE` 启用

---

#设计模式 #源码技巧
