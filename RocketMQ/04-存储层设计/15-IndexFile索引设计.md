# IndexFile 索引设计 — Hash 索引 + 链式冲突解决

> 📄 源码路径：`store/src/main/java/org/apache/rocketmq/store/index/`
> 🏷️ #核心类 #设计模式 #面试高频

## 🎯 核心问题

ConsumeQueue 只支持按 **Topic-Queue-Offset** 顺序消费，不支持按 Key 查询。
IndexFile 解决了"根据 MessageKey 或 UniqueKey 查找历史消息"的需求。

## 📐 整体结构 — 三段式文件

```
IndexFile 文件布局（默认 400MB）：
┌───────────────────────────────────────────────────────────────┐
│                      IndexHeader (40B)                         │
│  ┌─────────────┬─────────────┬──────────────┬──────────────┐  │
│  │BeginTimestamp│ EndTimestamp│BeginPhyOffset│EndPhyOffset  │  │
│  │   (8B)      │   (8B)      │   (8B)       │   (8B)       │  │
│  ├─────────────┴─────────────┴──────────────┴──────────────┤  │
│  │ HashSlotCount │ IndexCount│                              │  │
│  │    (4B)       │   (4B)    │                              │  │
│  └───────────────┴───────────┘                              │  │
├───────────────────────────────────────────────────────────────┤
│                   Hash Slot Table (500万 × 4B = 20MB)          │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────────────────┐    │
│  │Slot0│Slot1│Slot2│Slot3│Slot4│Slot5│ ... │Slot4999999│    │
│  │ 4B  │ 4B  │ 4B  │ 4B  │ 4B  │ 4B  │     │    4B     │    │
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────────────────┘    │
│  每个 Slot 存储该 Hash 桶中最新一条索引的序号                   │
├───────────────────────────────────────────────────────────────┤
│                   Index Data (2000万 × 20B = 400MB)           │
│  ┌──────────────────────────────────────────────────────┐    │
│  │ Index 0: [KeyHash(4B)][PhyOffset(8B)][TimeDiff(4B)][Next(4B)]│
│  │ Index 1: [KeyHash(4B)][PhyOffset(8B)][TimeDiff(4B)][Next(4B)]│
│  │ Index 2: [KeyHash(4B)][PhyOffset(8B)][TimeDiff(4B)][Next(4B)]│
│  │ ...                                                  │    │
│  │ Index 19999999: [...]                                │    │
│  └──────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────┘

文件总大小 = 40B + (500万 × 4B) + (2000万 × 20B) = 420MB ≈ 400MB
```

## 📐 IndexHeader — 40 字节文件头

```java
public class IndexHeader {
    // 40 字节固定头部
    private final AtomicLong beginTimestamp;    // 8B: 最早消息时间戳
    private final AtomicLong endTimestamp;      // 8B: 最新消息时间戳
    private final AtomicLong beginPhyOffset;    // 8B: 最早消息物理 offset
    private final AtomicLong endPhyOffset;      // 8B: 最新消息物理 offset
    private final AtomicInteger hashSlotCount;  // 4B: 已使用的 Hash 槽数
    private final AtomicInteger indexCount;     // 4B: 已写入的索引总数
}
```

> [!tip] Header 的作用
> - `beginTimestamp` / `endTimestamp`：查询时快速判断此文件是否包含目标时间段的消息
> - `beginPhyOffset` / `endPhyOffset`：判断物理 offset 范围
> - `hashSlotCount` / `indexCount`：管理文件容量

## 🔍 核心操作

### 1. putKey() — 写入索引

```java
public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
    // ① 计算 Hash 值和 Hash 槽位置
    int keyHash = indexKeyHashMethod(key);        // key 的 hashCode
    int slotPos = keyHash % this.hashSlotNum;     // 取模定位到哪个 Slot
    int absSlotPos = INDEX_HEADER_SIZE + slotPos * hashSlotSize;  // Slot 的绝对位置

    // ② 读取 Slot 中当前的值（链表头节点）
    int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
    if (slotValue <= 0 || slotValue > this.indexHeader.getIndexCount()) {
        slotValue = 0;  // 空桶
    }

    // ③ 计算时间差（秒级）
    long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
    timeDiff = timeDiff / 1000;

    // ④ 计算新索引的绝对位置
    int absIndexPos = INDEX_HEADER_SIZE
        + this.hashSlotNum * hashSlotSize          // 跳过 Hash Slot 表
        + this.indexHeader.getIndexCount() * indexSize;  // 当前索引序号 × 20

    // ⑤ 写入索引记录（20 字节）
    this.mappedByteBuffer.putInt(absIndexPos, keyHash);           // Key Hash (4B)
    this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);    // 物理 Offset (8B)
    this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff); // 时间差 (4B)
    this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);  // 前一个节点 (4B) ← 链表！

    // ⑥ 更新 Slot 指向新节点（头插法）
    this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

    // ⑦ 更新 Header
    if (invalidIndex == slotValue) {
        this.indexHeader.incHashSlotCount();  // 新桶 +1
    }
    this.indexHeader.incIndexCount();         // 索引总数 +1
    this.indexHeader.setEndPhyOffset(phyOffset);
    this.indexHeader.setEndTimestamp(storeTimestamp);

    return true;
}
```

### 📊 写入过程图解

```
假设：key="Order_123", keyHash=1000005, slotPos=5, indexCount=3

写入前：
  Slot[5] → 1 → 0    (已有一条索引在位置 1)

写入后：
  Slot[5] → 3 → 1 → 0
            ↑ 新写入的索引（位置 3）

  Index[3]:
  ┌──────────┬─────────────┬──────────┬──────────┐
  │KeyHash   │PhyOffset    │TimeDiff  │Prev=1    │
  │1000005   │102400       │3600      │1         │
  └──────────┴─────────────┴──────────┴──────────┘
```

> [!important] 头插法链表
> 每个 Hash Slot 存储的是**最新一条索引的序号**，
> 每条索引的 `Next Index Pos` 字段指向前一条同 Hash 的索引。
> 这是一个**单向链表**，新节点插在头部（头插法）。
>
> 好处：
> - 写入 O(1)：只需要更新 Slot 和新节点
> - 读取时从新到旧遍历，符合查询习惯

### 2. selectPhyOffset() — 查询索引

```java
public void selectPhyOffset(final List<Long> phyOffsets, final String key,
    final int maxNum, final long begin, final long end) {

    // ① 计算 Hash 和 Slot 位置
    int keyHash = indexKeyHashMethod(key);
    int slotPos = keyHash % this.hashSlotNum;
    int absSlotPos = INDEX_HEADER_SIZE + slotPos * hashSlotSize;

    // ② 获取链表头节点
    int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

    // ③ 遍历链表
    for (int nextIndexToRead = slotValue; ; ) {
        if (phyOffsets.size() >= maxNum) break;

        // 读取索引记录
        int absIndexPos = INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
            + nextIndexToRead * indexSize;

        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

        // 计算消息时间
        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff * 1000L;

        // ④ 时间范围匹配 + Hash 匹配
        boolean timeMatched = timeRead >= begin && timeRead <= end;
        if (keyHash == keyHashRead && timeMatched) {
            phyOffsets.add(phyOffsetRead);  // 命中！
        }

        // ⑤ 继续遍历前一个节点
        if (prevIndexRead <= 0 || prevIndexRead >= this.indexHeader.getIndexCount()
            || prevIndexRead == nextIndexToRead || timeRead < begin) {
            break;  // 到头了，或时间已经早于查询范围
        }
        nextIndexToRead = prevIndexRead;  // 链表遍历
    }
}
```

### 📊 查询过程图解

```
查询：key="Order_123", begin=1000, end=2000

① keyHash("Order_123") = 1000005
② slotPos = 1000005 % 5000000 = 5
③ Slot[5] = 3

遍历链表：
  Index[3]: keyHash=1000005, timeDiff=3600 → timeRead=4600 → 超出范围，跳过
  Index[1]: keyHash=1000005, timeDiff=1500 → timeRead=2500 → 超出范围，跳过
  Index[0]: keyHash=1000005, timeDiff=800  → timeRead=1800 → 命中！加入结果
  到头，结束。

结果：[Index[0] 对应的 phyOffset]
```

## 📐 IndexService — 索引管理器

### buildIndex() — 构建索引

```java
public void buildIndex(DispatchRequest req) {
    IndexFile indexFile = retryGetAndCreateIndexFile();

    // ① 按 UniqueKey 建索引（全局唯一消息 ID）
    if (req.getUniqKey() != null) {
        indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
    }

    // ② 按用户 Key 建索引（支持多个，用分隔符分割）
    if (keys != null && keys.length() > 0) {
        String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
        for (String key : keyset) {
            indexFile = putKey(indexFile, msg, buildKey(topic, key));
        }
    }

    // ③ 按 Tag 建索引（5.5.0 新增）
    if (propertiesMap.containsKey(MessageConst.PROPERTY_TAGS)) {
        String tags = req.getPropertiesMap().get(MessageConst.PROPERTY_TAGS);
        indexFile = putKey(indexFile, msg, buildKey(topic, tags, MessageConst.INDEX_TAG_TYPE));
    }
}
```

> [!important] 三种索引 Key
> | 类型 | Key 格式 | 用途 |
> |------|----------|------|
> | UniqueKey | `Topic#UNIQ_KEY` | 按消息唯一 ID 查询 |
> | UserKey | `Topic#userKey` | 按业务 Key 查询（如 orderId） |
> | Tag (5.5.0) | `Topic#TAG#tagName` | 按 Tag 查询 |

### queryOffset() — 查询消息

```java
public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
    List<Long> phyOffsets = new ArrayList<>();

    // 从最新的 IndexFile 开始往前找
    for (int i = this.indexFileList.size(); i > 0; i--) {
        IndexFile f = this.indexFileList.get(i - 1);

        // 时间范围匹配检查
        if (f.isTimeMatched(begin, end)) {
            String queryKey = buildKey(topic, key);
            f.selectPhyOffset(phyOffsets, queryKey, maxNum, begin, end);
        }

        // 优化：如果当前文件的 beginTimestamp 已经比 begin 早，不用继续往前找了
        if (f.getBeginTimestamp() < begin) {
            break;
        }

        if (phyOffsets.size() >= maxNum) break;
    }

    return new QueryOffsetResult(phyOffsets, ...);
}
```

## 📐 Hash 冲突处理 — 链地址法

```
IndexFile 使用链地址法（拉链法）解决 Hash 冲突：

Slot[0] → 0     (空)
Slot[1] → 5 → 2 → 0   (三条索引，Hash 冲突)
Slot[2] → 3 → 0
Slot[3] → 0
Slot[4] → 4 → 1 → 0
...

特点：
- 每个 Slot 是一条单向链表
- 新节点头插法（O(1)）
- 遍历时从新到旧
- 冲突时查询需要遍历链表（最坏 O(n)）
```

> [!note] 为什么不开放寻址？
> - 链地址法写入简单（O(1)），不需要探测
> - IndexFile 是只增不改的（append-only），不需要删除
> - 链表节点在物理上是连续的（按写入顺序），对缓存友好

## 📊 IndexFile vs ConsumeQueue

| 维度 | IndexFile | ConsumeQueue |
|------|-----------|-------------|
| 数据结构 | Hash 表 + 链表 | 定长数组 |
| 查询方式 | 按 Key/Tag | 按 Topic-Queue-Offset |
| 查询复杂度 | O(1) 平均，O(n) 最坏 | O(1) 确定 |
| 记录大小 | 20 字节 | 20 字节 |
| 用途 | 按 Key 查历史消息 | 消费者顺序消费 |
| 文件大小 | ~400MB | 30MB |
| 写入时机 | ReputMessageService 分发 | ReputMessageService 分发 |

## 💡 设计亮点

### 1. 时间差编码

```java
// 不存绝对时间戳（8字节），存相对 beginTimestamp 的秒级差值（4字节）
long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
timeDiff = timeDiff / 1000;  // 毫秒→秒

// 节省 4 字节 × 2000万条 = 80MB 空间！
```

### 2. 时间范围快速过滤

```java
// 查询时先判断时间范围，避免无效遍历
if (f.isTimeMatched(begin, end)) {
    f.selectPhyOffset(...);
}
if (f.getBeginTimestamp() < begin) {
    break;  // 文件时间已早于查询范围，不用继续
}
```

### 3. 文件命名即时间戳

```java
// IndexFile 文件名 = 创建时间
String fileName = this.storePath + File.separator
    + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
// 如: 20231201120000000

// 按文件名排序 = 按时间排序
Arrays.sort(files);
```

### 4. 异步刷盘 + Checkpoint

```java
// IndexFile 写满后，启动一个守护线程异步刷盘
Thread flushThread = new Thread(() -> {
    IndexService.this.flush(flushThisFile);  // flush 前一个写满的文件
}, "FlushIndexFileThread");
flushThread.setDaemon(true);
flushThread.start();

// 刷盘时更新 Checkpoint
this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
this.defaultMessageStore.getStoreCheckpoint().flush();
```

## 📊 查询完整链路

```
用户调用：queryMessage("TopicA", "Order_123", maxNum, beginTime, endTime)

① IndexService.queryOffset()
   → 构建查询 Key: "TopicA#Order_123"
   → 从最新 IndexFile 往前遍历
   → 调用 IndexFile.selectPhyOffset()

② IndexFile.selectPhyOffset()
   → keyHash("TopicA#Order_123") % 5000000 → Slot 位置
   → 从 Slot 链表头开始遍历
   → 匹配 Hash + 时间范围 → 收集 phyOffset

③ 根据 phyOffset 从 CommitLog 读取完整消息
   → commitLog.getData(phyOffset)

④ 返回消息给用户
```

---

## 💡 学习收获

1. **Hash + 链表**：经典的链地址法，适合 append-only 场景
2. **时间差编码**：用 4 字节存相对时间，节省大量空间
3. **头插法**：O(1) 写入，遍历时从新到旧
4. **三级索引**：UniqueKey + UserKey + Tag，覆盖常见查询场景
5. **异步刷盘**：写满后才刷，减少 I/O 次数
