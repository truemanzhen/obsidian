# CommitLog 写入流程 — 消息写入全链路

> 📄 源码路径：`store/src/main/java/org/apache/rocketmq/store/CommitLog.java`
> 🏷️ #核心类 #面试高频

## 📐 写入流程全景图

```
Producer 发送消息
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│ DefaultMessageStore.asyncPutMessage()                     │
│ ① 执行 PutMessageHook（前置钩子）                         │
│ ② 参数校验（batch flag、topic config）                    │
│ ③ 委托给 CommitLog.asyncPutMessage()                     │
└──────────────────────┬───────────────────────────────────┘
                       ▼
┌──────────────────────────────────────────────────────────┐
│ CommitLog.asyncPutMessage()                               │
│                                                          │
│  ① 设置 StoreTimestamp、BodyCRC、版本号                   │
│  ② HA 检查：判断同步副本数是否足够                         │
│  ③ TopicQueueLock.lock(topicQueueKey)  ← 粒度锁          │
│  ④ assignOffset(msg)  ← 分配全局 offset                  │
│  ⑤ encode(msg)  ← 编码消息为 ByteBuffer                  │
│  ⑥ PutMessageLock.lock()  ← 写入锁（自旋/可重入）         │
│  ⑦ MappedFile.appendMessage()  ← 追加到文件              │
│  ⑧ onCommitLogAppend()  ← 触发回调                       │
│  ⑨ PutMessageLock.unlock()                               │
│  ⑩ increaseOffset(msg)  ← 更新队列 offset                │
│  ⑪ TopicQueueLock.unlock()                               │
│  ⑫ handleDiskFlushAndHA()  ← 刷盘 + 主从同步             │
└──────────────────────────────────────────────────────────┘
```

## 🔍 关键步骤逐行解析

### 1. 双重锁设计

```java
// 锁 1：TopicQueueLock — 按 Topic-Queue 粒度
// 保证同一个 Queue 内的消息顺序性
topicQueueLock.lock(topicQueueKey);
try {
    // 锁 2：PutMessageLock — 全局写入锁
    // 保证 CommitLog 文件的写入原子性
    putMessageLock.lock();
    try {
        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
    } finally {
        putMessageLock.unlock();
    }
} finally {
    topicQueueLock.unlock();
}
```

> [!important] 为什么要两把锁？
> | 锁 | 粒度 | 作用 |
> |----|------|------|
> | TopicQueueLock | 按 Topic-Queue | 保证同一 Queue 的消息顺序性，分配 queueOffset |
> | PutMessageLock | 全局 | 保证 CommitLog 写入的原子性（同一时刻只有一个线程写文件） |
>
> TopicQueueLock 是**分段锁**（默认 32 个队列），减少竞争。
> PutMessageLock 可选**自旋锁**或**可重入锁**。

### 2. PutMessageLock — 两种实现

```java
// 自旋锁（默认）— 无上下文切换，适合临界区极短的场景
public class PutMessageSpinLock implements PutMessageLock {
    private final AtomicBoolean putMessageSpinLock = new AtomicBoolean(false);

    public void lock() {
        boolean flag;
        do {
            flag = this.putMessageSpinLock.compareAndSet(false, true);
        } while (!flag);
    }
}

// 可重入锁 — 适合临界区较长的场景
public class PutMessageReentrantLock implements PutMessageLock {
    private final ReentrantLock putMessageNormalLock = new ReentrantLock();

    public void lock() {
        this.putMessageNormalLock.lock();
    }
}

// 5.5.0 新增：自适应退避自旋锁（AdaptiveBackOffSpinLockImpl）
// 根据竞争激烈程度动态调整自旋策略
```

> [!tip] 自旋锁 vs 可重入锁
> | | 自旋锁 | 可重入锁 |
> |---|--------|---------|
> | 开销 | 无上下文切换 | 有线程挂起/唤醒 |
> | 适用 | 临界区 < 1μs | 临界区较长 |
> | 风险 | CPU 空转 | 线程切换开销 |
>
> CommitLog 的 appendMessage 操作极短（写几十字节到 ByteBuffer），所以默认用自旋锁。

### 3. 消息编码 — DefaultAppendMessageCallback

```java
class DefaultAppendMessageCallback implements AppendMessageCallback {

    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {

        // 计算物理 offset
        long wroteOffset = fileFromOffset + byteBuffer.position();

        // 消息格式（V1）：
        // ┌──────────┬──────────┬──────────┬──────────┬──────────┐
        // │ TOTALSIZE │ MAGICCODE │ BODYCRC  │  QUEUEID │ FLAG     │
        // │  (4B)    │  (4B)    │  (4B)    │  (4B)    │  (4B)    │
        // ├──────────┼──────────┼──────────┼──────────┼──────────┤
        // │ QUEUEOFFSET│ SYSFLAG  │ BORNTIMESTAMP│ BORNHOST│ ...
        // │  (8B)    │  (4B)    │  (8B)    │ (addr)   │          │
        // ├──────────┼──────────┼──────────┼──────────┼──────────┤
        // │ STORETIMESTAMP│ STOREHOST│ RECONSUMETIMES│ ...
        // │  (8B)    │ (addr)   │  (4B)    │          │          │
        // ├──────────┼──────────┼──────────┬──────────┼──────────┤
        // │ PROPERTIES│ BODY     │ TOPIC    │          │          │
        // │ (变长)    │ (变长)   │ (变长)   │          │          │
        // └──────────┴──────────┴──────────┴──────────┴──────────┘

        // 写入 ByteBuffer
        this.msgStoreItemMemory.putInt(msgLen);        // TOTALSIZE
        this.msgStoreItemMemory.putInt(MESSAGE_MAGIC_CODE); // MAGICCODE
        this.msgStoreItemMemory.putInt(bodyCRC);       // BODYCRC
        // ... 其他字段

        byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);
    }
}
```

### 4. END_OF_FILE 处理 — 文件写满切换

```java
result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
switch (result.getStatus()) {
    case PUT_OK:
        onCommitLogAppend(msg, result, mappedFile);
        break;
    case END_OF_FILE:
        // 当前文件剩余空间不够，切换到新文件
        onCommitLogAppend(msg, result, mappedFile);
        unlockMappedFile = mappedFile;
        // 获取或创建新文件（双缓冲已预分配）
        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        // 在新文件中重新写入
        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
        break;
}
```

> [!tip] END_OF_FILE 是正常情况
> 当文件剩余空间不足以放下一条消息时，会在文件末尾写入一个 **BLANK**（空白填充），
> 然后切换到下一个文件。BLANK 的 magic code 是 `cbd43194`。

### 5. 刷盘 + HA — handleDiskFlushAndHA

```java
// 写入完成后，处理刷盘和主从同步
return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);

// 内部调用链：
// handleDiskFlushAndHA()
//   → flushManager.handleDiskFlush()     // 刷盘
//   → haService.handleHA()               // 主从同步
//   → CompletableFuture<PutMessageStatus> // 异步返回结果
```

---

## 📊 完整时序图

```
Producer     DefaultMessageStore    CommitLog     MappedFile     FlushManager     HAService
  │                │                   │              │              │              │
  │ asyncPutMessage│                   │              │              │              │
  │───────────────→│                   │              │              │              │
  │                │ asyncPutMessage   │              │              │              │
  │                │──────────────────→│              │              │              │
  │                │                   │ topicQueueLock              │              │
  │                │                   │──┐           │              │              │
  │                │                   │  │ assignOffset             │              │
  │                │                   │←─┘           │              │              │
  │                │                   │ putMessageLock              │              │
  │                │                   │──┐           │              │              │
  │                │                   │  │ encode    │              │              │
  │                │                   │  │ appendMessage            │              │
  │                │                   │  │──────────→│              │              │
  │                │                   │  │ PUT_OK    │              │              │
  │                │                   │  │←──────────│              │              │
  │                │                   │←─┘           │              │              │
  │                │                   │              │              │              │
  │                │                   │ handleDiskFlushAndHA        │              │
  │                │                   │────────────────────────────→│              │
  │                │                   │                             │ handleHA     │
  │                │                   │                             │─────────────→│
  │                │                   │                             │              │
  │                │                   │ CompletableFuture<OK>       │              │
  │                │ CompletableFuture<OK>              │              │              │
  │  Result        │                   │              │              │              │
  │←───────────────│                   │              │              │              │
```

## 💡 关键设计总结

| 设计 | 在哪里 | 解决什么 |
|------|--------|----------|
| **双重锁** | TopicQueueLock + PutMessageLock | 顺序性 + 原子性 |
| **自旋锁** | PutMessageSpinLock | 极短临界区无上下文切换 |
| **分段锁** | TopicQueueLock(32) | 减少 Queue 间竞争 |
| **预分配** | AllocateMappedFileService | 文件切换零等待 |
| **END_OF_FILE** | BLANK 填充 | 优雅处理文件边界 |
| **异步刷盘** | FlushManager + CompletableFuture | 写入和刷盘解耦 |

---

> ⏭️ 下一篇：[[12-ConsumeQueue设计]] — 消费索引的精巧设计
