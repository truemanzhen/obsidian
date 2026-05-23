# ConsumeQueue 设计

> 研究定位：把 RocketMQ 的消费索引拆开看清楚，回答“为什么 ConsumeQueue 只存 20 字节，却能支撑高效顺序消费”。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/ConsumeQueue.java`、`ConsumeQueueStore.java`、`MappedFileQueue.java`、`ConsumeQueueExt.java`
> 阅读建议：这篇要和 [[28-CommitLog写入流程]] 配合看，ConsumeQueue 只是索引，不是消息本体。

## 先给结论

- ConsumeQueue 是按 `topic + queueId` 组织的逻辑消费索引，不是全局索引。
- 它每条记录固定 20 字节，核心字段只有 `commitlog offset + msg size + tagsCode`。
- 它的目标不是保存完整消息，而是让消费端以 O(1) 找到某条消息在 CommitLog 的物理位置。
- 5.5.0 还支持 `ConsumeQueueExt`，用来承载更复杂的扩展过滤信息。
- 逻辑窗口的有效性始终受 CommitLog 的物理保留窗口约束。

## 为什么它必须存在

CommitLog 是全局顺序追加日志。

如果消费端直接去扫 CommitLog，会遇到三个问题：

1. [[28-CommitLog写入流程]]全局日志太大，定位慢
2. [[30-ReputMessageService异步分发]]消费队列语义丢失
3. [[36-消息过滤机制]]过滤和 offset 组织都会变复杂

所以 ConsumeQueue 解决的是：

- 把“全局物理日志”变成“按 topic-queue 组织的消费索引”

## 20 字节固定结构

每条 ConsumeQueue 记录固定 20 字节：

- `commitLogOffset`：8B
- `msgSize`：4B
- `tagsCode`：8B

这个设计的价值很直接：

- 结构固定，位置可直接按 offset 计算
- 读写都非常轻
- 可以按队列顺序快速定位到 CommitLog

## 为什么不是把更多字段也存进去

因为 ConsumeQueue 的职责不是“完整消息索引”，而是“消费入口索引”。

它只需要回答：

- 去 CommitLog 的哪里读
- 这条消息有多大
- 是否可能被 tag 快速过滤

其他内容都留在 CommitLog 里。

## 文件结构是按 topic 和 queueId 分层的

典型路径：

```text
store/consumequeue/<topic>/<queueId>/<offsetFile>
```

其中每个文件定长，文件名本身就是起始 offset。

这意味着 ConsumeQueue 也继承了 `MappedFileQueue` 的那套优势：

- 定位快
- 滚动简单
- 恢复可控

## 核心写入链

ConsumeQueue 不是生产者写进去的，而是 `ReputMessageService` 分发时构建的。

主链路是：

1. [[28-CommitLog写入流程]]CommitLog 写入成功
2. [[30-ReputMessageService异步分发]]`ReputMessageService` 解析新消息
3. [[36-消息过滤机制]]`CommitLogDispatcherBuildConsumeQueue` 写入索引

也就是说：

- ConsumeQueue 是异步跟随 CommitLog 构建的

## 查询链路

消费时大概是这样：

1. [[28-CommitLog写入流程]]用 `queueOffset` 找到对应 20B 记录
2. [[30-ReputMessageService异步分发]]读出 `commitLogOffset` 和 `msgSize`
3. [[36-消息过滤机制]]回跳到 CommitLog 读完整消息

所以 ConsumeQueue 的本质是：

- “逻辑 offset -> 物理 offset” 的桥

## `tagsCode` 的真正用途

`tagsCode` 不是完整 Tag，而是给快速过滤准备的辅助值。

两层过滤的关系是：

### 第一层：ConsumeQueue 过滤

- 先用 `tagsCode` 做粗过滤

### 第二层：CommitLog 过滤

- 读到完整消息后，再做精确过滤

这样做的原因很简单：

- 先用轻量索引降低无效读取
- 再用完整消息做最终判断

## `ConsumeQueueExt` 是什么

`ConsumeQueueExt` 是扩展索引存储。

它的存在说明：

- 20 字节主结构足够应付大多数高频消费场景
- 但部分高级过滤或扩展信息不能再塞回固定主结构里

所以 5.5.0 保持了：

- 主索引极简
- 扩展信息外置

## 恢复时为什么要校正 minOffset

`ConsumeQueue.recover()` 会从文件尾部往前找有效数据，并更新：

- `maxPhysicOffset`
- `flushedWhere`
- `committedWhere`

最后再 `truncateDirtyFiles(...)`。

这说明恢复不是简单“重新加载文件”，而是：

- 找到最后有效消费窗口
- 截断坏尾
- 对齐物理窗口

## 时间查询为什么也走 ConsumeQueue

`getOffsetInQueueByTime(...)` 不是扫 CommitLog，而是：

1. [[28-CommitLog写入流程]]先用 `MappedFileQueue.getConsumeQueueMappedFileByTime(...)` 找候选文件
2. [[30-ReputMessageService异步分发]]再二分查找具体 offset

这是一种典型的“文件级粗定位 + 文件内细定位”模式。

## 这篇最值得记住的点

- ConsumeQueue 是消费索引，不是消息存储。
- 它的 20 字节结构是 RocketMQ 高效顺序消费的核心之一。
- 它必须跟随 CommitLog 存在，不能单独解释。
- `tagsCode` 只是粗过滤，不是最终语义。

## 和后续笔记的关系

-  会解释谁在写 ConsumeQueue。
-  会解释两级过滤怎么配合。
-  会解释为什么它的最小 offset 会被物理窗口约束。

## 建议连读

1. [[28-CommitLog写入流程]]
2. [[30-ReputMessageService异步分发]]
3. [[36-消息过滤机制]]
4. [[33-消息存储清理与回溯边界]]