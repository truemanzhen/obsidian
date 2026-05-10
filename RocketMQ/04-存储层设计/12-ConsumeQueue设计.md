# ConsumeQueue 设计 — 消费索引的精巧设计

> 📄 源码路径：`store/src/main/java/org/apache/rocketmq/store/ConsumeQueue.java`
> 🏷️ #核心类 #面试高频

## 🎯 核心思想

ConsumeQueue 是 CommitLog 的**消费索引**，解决了"CommitLog 所有消息混存，消费者如何高效读取自己关心的消息"的问题。

## 📐 存储格式 — 20 字节定长

```
每条 ConsumeQueue 记录固定 20 字节：

┌───────────────────────┬─────────────┬───────────────────┐
│ CommitLog Offset (8B) │ Size (4B)   │ Tag HashCode (8B) │
└───────────────────────┴─────────────┴───────────────────┘

- CommitLog Offset：消息在 CommitLog 中的物理偏移量
- Size：消息大小
- Tag HashCode：消息 Tag 的 Hash 值（用于消费过滤）
```

> [!important] 为什么是 20 字节？
> - **定长**：可以通过 offset 直接计算位置 → O(1) 随机访问
> - **极小**：30MB 文件可以存 150 万条索引
> - **够用**：只存定位信息，不存消息内容

## 📐 文件组织

```
store/consumequeue/
└── TopicA/
    ├── 0/                          ← Queue 0
    │   ├── 00000000000000000000    ← 第 1 个文件（30MB）
    │   │   ├── [0] offset=0, size=256, tagHash=0xABC
    │   │   ├── [1] offset=256, size=128, tagHash=0xDEF
    │   │   ├── [2] offset=384, size=256, tagHash=0x123
    │   │   └── ...（每条 20 字节）
    │   └── 00000000000000600000    ← 第 2 个文件
    └── 1/                          ← Queue 1
        └── ...
```

## 🔍 核心操作

### 1. 按 queueOffset 查找 — O(1)

```java
// queueOffset 是第几条消息
// 每条 20 字节，所以文件内偏移 = queueOffset * 20
long fileOffset = queueOffset * CQ_STORE_UNIT_SIZE;  // 20

// 找到对应的 MappedFile
MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(fileOffset, true);

// 读取 20 字节
SelectMappedBufferResult result = mappedFile.selectMappedBuffer(
    (int) (fileOffset % mappedFileSize), CQ_STORE_UNIT_SIZE);

// 解析
ByteBuffer buffer = result.getByteBuffer();
long offsetPy = buffer.getLong();      // CommitLog Offset (8B)
int sizePy = buffer.getInt();          // Size (4B)
long tagsCode = buffer.getLong();      // Tag HashCode (8B)
```

> [!tip] O(1) 查找链路
> ```
> queueOffset=1000
>   → 文件偏移 = 1000 * 20 = 20000
>   → 第几个文件 = 20000 / 30MB = 0
>   → 文件内偏移 = 20000 % 30MB = 20000
>   → 直接读取 20 字节
> ```
> 全程除法+取模，没有遍历。

### 2. 消费过滤 — Tag HashCode

```java
// 消费者指定 Tag 过滤
if (tagsCode != 0 && !messageFilter.isMatchedByConsumeQueue(tagsCode)) {
    // Tag 不匹配，跳过
    continue;
}

// 读取完整消息后，再做精确 Tag 匹配
MessageExt msgExt = lookMessageByOffset(offsetPy, sizePy);
if (!messageFilter.isMatchedByCommitLog(msgExt, bufferConsumeQueue)) {
    // 精确过滤
}
```

> [!note] 两级过滤
> 1. **ConsumeQueue 级**：用 Tag HashCode 快速过滤（可能有 Hash 碰撞）
> 2. **CommitLog 级**：读取完整消息后精确匹配
>
> 这样可以避免大量无用消息的读取。

### 3. 写入索引 — putMessagePositionInfo

```java
// 由 ReputMessageService 异步调用
public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
    // 构建 20 字节索引
    this.byteBufferIndex.putLong(dispatchRequest.getCommitLogOffset());  // 8B
    this.byteBufferIndex.putInt(dispatchRequest.getMsgSize());           // 4B
    this.byteBufferIndex.putLong(dispatchRequest.getTagsCode());         // 8B

    // 追加到 MappedFile
    boolean result = this.putMessagePositionInfoRelative(
        dispatchRequest.getCommitLogOffset(),
        dispatchRequest.getMsgSize(),
        dispatchRequest.getTagsCode());
}
```

## 📐 ConsumeQueue vs IndexFile

| | ConsumeQueue | IndexFile |
|---|---|---|
| 组织方式 | 按 Topic-Queue | 按 Key Hash |
| 记录大小 | 20 字节定长 | 变长 |
| 查询方式 | offset → 物理位置 | Key → 物理位置 |
| 用途 | 消费者顺序消费 | 按 Key 查询历史消息 |
| 数据量 | 每个 Topic-Queue 一个 | 全局共享 |

## 💡 设计亮点

### 1. 极简的索引结构

```
只存 3 个字段：
- 在哪（CommitLog Offset）
- 多大（Size）
- 是什么（Tag HashCode）

不存 Topic、QueueId、消息体 → 索引极小
```

### 2. 全量加载到内存

```
假设：
- 100 个 Topic
- 每个 Topic 4 个 Queue
- 每个 Queue 30MB ConsumeQueue

总大小 = 100 * 4 * 30MB = 12GB ← 不现实

但实际上：
- 活跃的 Topic-Queue 通常不多
- ConsumeQueue 文件很小（30MB）
- OS 会自动用 PageCache 缓存热点文件
```

> [!tip] PageCache 加速
> ConsumeQueue 文件通常只有几十 MB，OS 会自动缓存。
> 消费者读取消息时，ConsumeQueue 查找几乎**零磁盘 I/O**。

### 3. Tag HashCode 的取舍

```
用 Hash 而不是完整 Tag 的好处：
- 节省空间（8B vs 变长字符串）
- 支持快速过滤

代价：
- 有 Hash 碰撞（需要二次校验）
- 不支持范围查询
```

## 📊 消费流程完整链路

```
Consumer: "给我 TopicA-Q0 的第 1000~1010 条消息"

① ConsumeQueue 查找
   offset=1000 → 文件偏移=20000 → 读取 20B 索引
   得到：CommitLogOffset=102400, Size=256, TagHash=0xABC

② Tag 过滤（ConsumeQueue 级）
   TagHash 匹配 → 通过

③ CommitLog 查找
   offset=102400 → 读取 256B → 得到完整消息

④ 精确 Tag 匹配（CommitLog 级）
   完整 Tag 匹配 → 通过

⑤ 返回给 Consumer
```

---

> ⏭️ 下一篇：[[13-ReputMessageService异步分发]] — CommitLog → ConsumeQueue 的桥梁
