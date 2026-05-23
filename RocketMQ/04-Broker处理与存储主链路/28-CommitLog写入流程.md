# CommitLog 写入流程

> 研究定位：只看默认存储引擎里“消息如何变成字节并真正落入 CommitLog”这条主链，重点区分排队顺序、物理写入、刷盘和 HA 这四件事。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java`、`CommitLog.java`

## 先给结论

- 默认写入主链是 `DefaultMessageStore.asyncPutMessage(...) -> CommitLog.asyncPutMessage(...)`。
- 写入过程中至少有三类状态要区分：追加结果、刷盘结果、HA 结果。
- `TopicQueueLock` 负责同队列顺序，`PutMessageLock` 负责物理写入临界区。
- `END_OF_FILE` 不是异常，而是 CommitLog 文件切换的正常分支。

## 写入主线

入口从 `DefaultMessageStore` 开始：

```java
public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
    return this.commitLog.asyncPutMessage(msg);
}
```

真正的主逻辑在 `CommitLog.asyncPutMessage(...)`。

## 四个关键阶段

### 1. 写前准备

这一阶段会处理：

- store timestamp
- body crc
- topic/queue 上下文
- 需要的 HA / flush 条件判断

### 2. 顺序与锁

这里最关键的是两把锁：

- `TopicQueueLock`
- `PutMessageLock`

可以把它们理解成：

- 前者保证同一个 topic-queue 的逻辑顺序推进
- 后者保证物理日志追加时的临界区原子性

## 两把锁不是重复设计

### `TopicQueueLock`

它绑定的是：

- topic
- queueId

解决的是：

- queue offset 分配和同队列顺序性

### `PutMessageLock`

它保护的是：

- 当前 mapped file 的追加写入

5.5.0 里这条锁链已经不止单一实现，常见至少有：

- 自旋锁
- 可重入锁
- 自适应退避自旋锁

所以写入竞争优化是这一层的重要主题，不是外围细节。

## 真正把消息变成字节的是 append callback

源码里物理布局并不是在业务代码里手工 `putInt/putLong` 拼完就结束，而是交给：

- `DefaultAppendMessageCallback`

它负责写入：

- 总长度
- magic code
- queue offset
- physical offset
- born/store timestamp
- body
- topic
- properties

所以研究 CommitLog 时，要把“消息编码格式”视为写入主线的一部分。

## 文件切换是正常路径

当当前文件剩余空间不足时，会出现：

- `AppendMessageStatus.END_OF_FILE`

此时流程会：

1. 写入文件尾部空白占位
2. 切到下一个 mapped file
3. 重新 append 当前消息

这不是失败重试，而是正常滚动。

## 追加成功不等于全部完成

消息 append 成功后，后面还有两段结果需要等待：

- `handleDiskFlush(...)`
- `handleHA(...)`

也就是说，最终返回的 `PutMessageResult` 不只是“字节已经写进 ByteBuffer”。

它还会受：

- 同步/异步刷盘策略
- 同步副本策略

影响。

## 这条链里最容易混淆的三种结果

### 1. 追加结果

说明：

- 字节有没有成功写入当前 commitlog 文件

### 2. 刷盘结果

说明：

- 写入是否达到配置要求的持久化语义

### 3. HA 结果

说明：

- 是否满足同步复制或副本确认要求

所以“发送成功”不是一个单点概念，而是多阶段聚合结果。

## 这篇最该记住的边界

- CommitLog 只做顺序物理日志追加，不直接等于消费索引完成。
- 逻辑分发到 `ConsumeQueue` / `Index` 是后续阶段。
- 文件边界、刷盘策略、HA 策略，都会影响最终写入观察结果。

## 关联阅读

- [[30-ReputMessageService异步分发]]