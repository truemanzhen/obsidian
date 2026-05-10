# RocksDB 集成设计

> 源码位置：`store/src/main/java/org/apache/rocketmq/store/` + `tieredstore/`

## 🏗️ 为什么引入 RocksDB

RocketMQ 5.x 大规模引入 RocksDB，替代原有的基于文件的存储方案：

| 场景 | 原方案 | RocksDB 方案 | 优势 |
|---|---|---|---|
| ConsumeQueue | 定长文件 + MappedByteBuffer | RocksDB KV | 更好的随机读、自动 Compaction |
| IndexFile | 自定义哈希索引 | RocksDB Index | 更灵活的索引、无固定容量 |
| 事务消息 | 自定义文件格式 | RocksDB | 事务状态原子更新 |
| Timer 消息 | 自定义 TimerLog | RocksDB | 时间轮持久化更可靠 |
| TieredStore | - | RocksDB + 云存储 | 冷热分离 |

## 📦 核心组件

### 1. ConsumeQueue 存储

```
原方案：ConsumeQueue（定长 20 字节 × N 条）
  └── 每条：offset(8) + size(4) + tagHashcode(8)

RocksDB 方案：
  ├── RocksDBConsumeQueueStore：Store 层入口
  ├── RocksDBConsumeQueueTable：队列表（topic:queueId → 消息列表）
  ├── RocksDBConsumeQueueOffsetTable：消费进度表
  └── ConsumeQueueRocksDBStorage：底层 RocksDB 操作封装
```

**Key 设计：**

```java
// ConsumeQueue Key 格式
// topicId + queueId + offset → messageInfo
public class RocksDBConsumeQueue {
    // 存储格式：[topicId(4) | queueId(4) | offset(8)] → [msgOffset(8) | msgSize(4) | tagHash(8)]
}
```

**优势：**
- 不再需要定长 20 字节限制
- 支持动态增减队列
- 自动 Compaction 清理过期数据
- 更好的随机读性能

### 2. Index 索引存储

```
原方案：IndexFile（固定大小哈希表 + 链表）
  └── 每个 IndexFile 固定 4000 万条目，用完即止

RocksDB 方案：
  └── IndexRocksDBStore
      └── IndexRocksDBRecord：key=topic+keyHash → value=msgOffset+timestamp
```

**优势：**
- 无固定容量限制
- 自动扩容
- 更好的查询性能

### 3. 事务消息存储

```
原方案：TransactionalMessageServiceImpl + 自定义文件
  └── 半消息存储在特殊 Topic（RMQ_SYS_TRANS_HALF_TOPIC）

RocksDB 方案：
  └── TransMessageRocksDBStore
      └── TransRocksDBRecord：事务状态原子存储
```

**关键改进：**
- 事务状态（COMMIT/ROLLBACK）原子更新
- 回查状态持久化
- 更可靠的事务恢复

### 4. Timer 消息存储

```
原方案：TimerWheel + TimerLog（自定义文件格式）
  └── 时间轮持久化到 TimerLog

RocksDB 方案：
  └── TimerMessageRocksDBStore
      └── TimerRocksDBRecord：时间点 → 消息引用
```

**关键改进：**
- 时间轮数据更可靠
- 崩溃恢复更快
- 支持更细粒度的时间精度

## 🔧 底层封装

### RocksDBOptionsFactory

```java
public class RocksDBOptionsFactory {
    // 统一的 RocksDB 配置工厂
    public static Options createOptions() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setWriteBufferSize(64 * 1024 * 1024);  // 64MB write buffer
        options.setMaxWriteBufferNumber(3);
        options.setLevel0FileNumCompactionTrigger(10);
        options.setTargetFileSizeBase(64 * 1024 * 1024);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        return options;
    }
}
```

### MessageRocksDBStorage

```java
public class MessageRocksDBStorage {
    private RocksDB db;
    private final String dbPath;

    // 基础操作
    public void put(byte[] key, byte[] value);
    public byte[] get(byte[] key);
    public void delete(byte[] key);

    // 批量操作（提高性能）
    public void batchPut(Map<byte[], byte[]> kvs);
    public WriteBatch createWriteBatch();

    // 迭代器
    public RocksIterator createIterator();
}
```

### RocksDBGroupCommitService

```java
// 批量提交服务，减少 RocksDB 写入次数
public class RocksGroupCommitService extends ServiceThread {
    private final List<CommitLogEntry> requests = new ArrayList<>();

    public void put(CommitLogEntry entry) {
        synchronized (requests) {
            requests.add(entry);
            requests.notify();
        }
    }

    @Override
    public void run() {
        while (!isStopped()) {
            List<CommitLogEntry> batch;
            synchronized (requests) {
                if (requests.isEmpty()) {
                    requests.wait();
                }
                batch = new ArrayList<>(requests);
                requests.clear();
            }
            // 批量写入 RocksDB
            db.write(writeOptions, writeBatch);
        }
    }
}
```

## 📊 存储类型切换

### 配置项

```properties
# 存储类型选择
storeType=DEFAULT          # 默认：文件存储
storeType=ROCKSDB          # RocksDB 存储
storeType=TIERED_STORAGE   # 分层存储（RocksDB + 云存储）
```

### DefaultMessageStore vs RocksDBMessageStore

```java
// 根据配置创建不同的 MessageStore
public class MessageStoreBuilder {
    public static MessageStore build(MessageStoreConfig config) {
        switch (config.getStoreType()) {
            case ROCKSDB:
                return new RocksDBMessageStore(config);
            case TIERED_STORAGE:
                return new TieredMessageStore(config);
            default:
                return new DefaultMessageStore(config);
        }
    }
}
```

### RocksDBMessageStore

```java
public class RocksDBMessageStore implements MessageStore {
    // 使用 RocksDB 存储 CommitLog
    private MessageRocksDBStorage commitLogStorage;

    // 使用 RocksDB 存储 ConsumeQueue
    private RocksDBConsumeQueueStore consumeQueueStore;

    // 使用 RocksDB 存储索引
    private IndexRocksDBStore indexStore;

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // 1. 写入 CommitLog（RocksDB）
        commitLogStorage.put(msgKey, msgBody);

        // 2. 分发到 ConsumeQueue（RocksDB）
        consumeQueueStore.putConsumeQueue(topic, queueId, offset, size, tagHash);

        // 3. 更新索引（RocksDB）
        indexStore.put(msgId, topic, key, timestamp, offset);

        return new PutMessageResult(PutMessageStatus.PUT_OK, ...);
    }
}
```

## 🏝️ TieredStore（分层存储）

5.x 新特性，支持冷热数据分层：

```
┌─────────────────────────────────────────────────┐
│                  TieredStore                     │
├─────────────────────────────────────────────────┤
│  Hot Tier (SSD/NVMe)                            │
│  └── 最近的消息，高频访问                         │
├─────────────────────────────────────────────────┤
│  Warm Tier (HDD)                                │
│  └── 中等年龄的消息，中频访问                     │
├─────────────────────────────────────────────────┤
│  Cold Tier (S3/OSS)                             │
│  └── 历史消息，低频访问                           │
└─────────────────────────────────────────────────┘
```

### 核心类

| 类 | 职责 |
|---|---|
| `FileSegment` | 文件分段抽象 |
| `PosixFileSegment` | 本地文件分段 |
| `MemoryFileSegment` | 内存文件分段 |
| `FileSegmentFactory` | 文件分段工厂 |
| `MessageStoreFetcher` | 消息读取接口 |
| `MessageStoreDispatcher` | 消息分发接口（写入） |

### 数据流

```
写入：
Producer → MessageStoreDispatcher
    ├── Hot Tier (SSD)：最近 24h 消息
    ├── Warm Tier (HDD)：24h-7d 消息
    └── Cold Tier (S3)：7d 以上消息

读取：
Consumer → MessageStoreFetcher
    ├── 先查 Hot Tier
    ├── 再查 Warm Tier
    └── 最后查 Cold Tier（异步拉取）
```

## 💡 设计亮点

1. **存储引擎可插拔**：通过 StoreType 配置切换，业务代码无感知
2. **Group Commit**：批量写入减少 RocksDB I/O 次数
3. **LZ4 压缩**：减少存储空间，提高读取速度
4. **自动 Compaction**：RocksDB 自动清理过期数据
5. **冷热分层**：TieredStore 自动管理数据生命周期
6. **事务原子性**：利用 RocksDB 的 WriteBatch 保证事务操作原子

## ⚠️ 注意事项

1. **磁盘空间**：RocksDB 的 Compaction 需要额外磁盘空间（约 2-3 倍数据量）
2. **写放大**：Compaction 会导致写放大，注意 SSD 寿命
3. **内存占用**：RocksDB Block Cache 会占用一定内存
4. **配置调优**：需要根据实际负载调整 writeBufferSize、maxWriteBufferNumber 等参数
5. **备份恢复**：RocksDB 的备份机制与传统文件不同，需要使用 RocksDB 原生的 Checkpoint/Backup

## 🔗 与其他模块的关系

- **Store 层**：RocksDB 是底层存储引擎，被 DefaultMessageStore/RocksDBMessageStore 使用
- **ConsumeQueue**：从定长文件迁移到 RocksDB KV
- **事务消息**：事务状态存储在 RocksDB
- **Timer 消息**：时间轮数据持久化到 RocksDB
- **TieredStore**：基于 RocksDB 实现冷热分层

---

#核心类 #源码技巧 #设计模式
