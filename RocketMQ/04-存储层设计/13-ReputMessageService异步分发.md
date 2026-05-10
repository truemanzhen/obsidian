# ReputMessageService 异步分发 — CommitLog → ConsumeQueue 的桥梁

> 📄 源码路径：`store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java`（内部类）
> 🏷️ #核心类 #设计模式

## 🎯 职责

`ReputMessageService` 是一个**后台服务线程**，持续从 CommitLog 读取新写入的消息，
解析出 DispatchRequest，然后分发给各个 Dispatcher 构建索引（ConsumeQueue、IndexFile 等）。

## 📐 分发架构

```
┌─────────────────────────────────────────────────────────────┐
│                    ReputMessageService                       │
│                    (后台常驻线程)                              │
│                                                             │
│  reputFromOffset ──────────────────────→ CommitLog MaxOffset │
│  (已分发到哪)                           (最新写入位置)        │
│                                                             │
│  循环：                                                      │
│  ① commitLog.getData(reputFromOffset)  读取 CommitLog 数据  │
│  ② checkMessageAndReturnSize()         解析消息              │
│  ③ doDispatch(dispatchRequest)         分发给 Dispatcher     │
│  ④ reputFromOffset += size             移动指针              │
└────────────────────────┬────────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ CQ Dispatcher│ │Index Dispatcher│ │Trans Dispatcher│
│              │ │              │ │              │
│ 构建         │ │ 构建          │ │ 构建          │
│ ConsumeQueue │ │ IndexFile    │ │ 事务索引      │
└──────────────┘ └──────────────┘ └──────────────┘
```

## 🔍 源码逐行

### doReput() — 核心循环

```java
public void doReput() {
    // 安全检查：如果 reputFromOffset < CommitLog 最小 offset，说明分发严重落后
    if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
        LOGGER.warn("The reputFromOffset={} is smaller than minPyOffset={}", ...);
        this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
    }

    // 主循环：持续读取 CommitLog 中的新消息
    for (boolean doNext = true; isCommitLogAvailable() && doNext; ) {

        // ① 从 CommitLog 读取数据（从 reputFromOffset 开始）
        SelectMappedBufferResult result =
            DefaultMessageStore.this.commitLog.getData(reputFromOffset);

        if (result == null) {
            break;  // 没有新数据
        }

        try {
            this.reputFromOffset = result.getStartOffset();

            // ② 逐条解析消息
            for (int readSize = 0; readSize < result.getSize()
                && reputFromOffset < getReputEndOffset() && doNext; ) {

                // ③ 解析消息，构建 DispatchRequest
                DispatchRequest dispatchRequest =
                    DefaultMessageStore.this.commitLog
                        .checkMessageAndReturnSize(result.getByteBuffer(), false, false, false);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        currentReputTimestamp = dispatchRequest.getStoreTimestamp();

                        // ④ 分发！
                        DefaultMessageStore.this.doDispatch(dispatchRequest);

                        // ⑤ 通知消息到达（长轮询）
                        notifyMessageArriveIfNecessary(dispatchRequest);

                        // ⑥ 移动指针
                        this.reputFromOffset += size;
                        readSize += size;
                    } else if (size == 0) {
                        // 遇到文件末尾的 BLANK，跳到下一个文件
                        this.reputFromOffset = DefaultMessageStore.this.commitLog
                            .rollNextFile(this.reputFromOffset);
                    }
                }
            }
        } finally {
            result.release();  // 释放 ByteBuffer 引用
        }
    }
}
```

### doDispatch() — 分发给各 Dispatcher

```java
public void doDispatch(DispatchRequest req) throws RocksDBException {
    for (CommitLogDispatcher dispatcher : this.dispatcherList) {
        dispatcher.dispatch(req);
    }
}
```

### 三个 Dispatcher

```java
// ① 构建 ConsumeQueue 索引
class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {
    public void dispatch(DispatchRequest request) {
        // 只处理已提交的消息（事务消息 PREPARED/ROLLBACK 不处理）
        switch (tranType) {
            case TRANSACTION_NOT_TYPE:
            case TRANSACTION_COMMIT_TYPE:
                putMessagePositionInfo(request);  // 写入 ConsumeQueue
                break;
            case TRANSACTION_PREPARED_TYPE:
            case TRANSACTION_ROLLBACK_TYPE:
                break;  // 跳过
        }
    }
}

// ② 构建 IndexFile 索引
class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {
    public void dispatch(DispatchRequest request) {
        if (messageStoreConfig.isMessageIndexEnable()) {
            indexService.buildIndex(request);  // 写入 IndexFile
        }
    }
}

// ③ 构建事务索引
class CommitLogDispatcherBuildTransIndex implements CommitLogDispatcher {
    public void dispatch(DispatchRequest request) {
        transMessageRocksDBStore.buildTransIndex(request);  // 写入 RocksDB
    }
}
```

## 📐 分发与写入的时序关系

```
时间线 ──────────────────────────────────────────────→

Producer 写入：
  [Msg0] [Msg1] [Msg2] [Msg3] [Msg4] ...
         ↑
    CommitLog（顺序写）

ReputMessageService 分发（异步，有延迟）：
         [Msg0] [Msg1] [Msg2] ...
              ↑
    ConsumeQueue（异步构建）

Consumer 消费：
              [Msg0] [Msg1] ...
                   ↑
    只能消费已分发的消息
```

> [!important] 分发延迟
> 消息写入 CommitLog 后，需要等 ReputMessageService 分发到 ConsumeQueue 后才能被消费。
> 这就是 `dispatchBehindBytes()` 指标的含义：**还有多少字节未分发**。
>
> 正常情况下延迟在毫秒级，但在高写入场景下可能积压。

## 📊 ReputMessageService 状态监控

```java
// 分发延迟（字节数）
long behindBytes = dispatchBehindBytes();
// = CommitLog.maxOffset - reputFromOffset

// 分发延迟（毫秒）
long behindMs = reputMessageService.behindMs();
// = CommitLog最后文件的storeTimestamp - currentReputTimestamp
```

> [!tip] 监控指标
> - `dispatchBehindBytes == 0`：分发追上了写入
> - `dispatchBehindBytes > 0`：分发落后，消费者可能读不到最新消息
> - Broker 启动时会**等待分发完成**再对外服务

## 💡 设计亮点

### 1. 生产者-消费者模式

```
Producer（生产者）：
  写入 CommitLog → 顺序写，速度极快

ReputMessageService（消费者）：
  读取 CommitLog → 解析 → 分发到 ConsumeQueue/IndexFile

两者解耦：
  - 写入不需要等待索引构建
  - 索引构建不影响写入性能
```

### 2. 单线程分发

```
ReputMessageService 是单线程：
  - 避免 ConsumeQueue 写入的并发问题
  - 保证消息分发的顺序性
  - 简化实现
```

> [!note] 为什么不用多线程分发？
> ConsumeQueue 的写入是**追加写**（append-only），需要保证顺序。
> 如果多线程分发，需要额外的排序逻辑，得不偿失。

### 3. 读取粒度控制

```java
// 读取范围由 getReputEndOffset() 控制
protected long getReputEndOffset() {
    // 配置项：isReadUnCommitted
    // true:  读到 CommitLog 的 maxOffset（未刷盘的消息也分发）
    // false: 读到 CommitLog 的 confirmOffset（只分发已确认的消息）
    return isReadUnCommit ? commitLog.getMaxOffset() : commitLog.getConfirmOffset();
}
```

---

> ⏭️ 下一篇：[[14-刷盘机制]] — 消息持久化的保证
