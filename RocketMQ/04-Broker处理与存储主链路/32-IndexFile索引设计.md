# IndexFile 索引设计

> 研究定位：把 RocketMQ 按 key 查消息的辅助索引讲清楚，回答“为什么消费主链不依赖它，但生产环境里它仍然很重要”。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/index/IndexFile.java`、`IndexHeader.java`、`IndexService.java`
> 阅读建议：这篇要和 [[29-ConsumeQueue设计]] 对照看，两个都是索引，但职责完全不同。

## 先给结论

- IndexFile 是按 key 查询历史消息的辅助索引，不参与主消费路径。
- 它的底层结构是 `Hash Slot + Index Entry + 链式冲突处理`。
- 每条索引记录也是固定 20 字节，但语义和 ConsumeQueue 完全不同。
- 5.5.0 里 `IndexService` 不只给 uniqKey 和业务 key 建索引，也支持 tag 索引。
- IndexFile 会随着 CommitLog 清理而裁剪，所以它不保证无限历史可查。

## 为什么要有它

ConsumeQueue 只能回答：

- 某个 topic-queue 的第 N 条消息在哪

但很多场景需要：

- 按业务 key 查消息
- 按 uniqKey 找发送结果

这时就需要独立辅助索引。

## 三段式文件布局

一个 IndexFile 大体分三部分：

1. [[29-ConsumeQueue设计]]`IndexHeader`
2. [[30-ReputMessageService异步分发]]Hash Slot 区
3. Index Entry 区

你可以把它理解成：

- 头部记录时间和偏移范围
- slot 做哈希入口
- entry 做真正链式节点

## `IndexHeader` 记录的是什么

最关键的字段包括：

- `beginTimestamp`
- `endTimestamp`
- `beginPhyOffset`
- `endPhyOffset`
- `hashSlotCount`
- `indexCount`

这几个字段不是装饰信息，而是：

- 查询剪枝
- 文件边界判断
- 恢复和滚动依据

其中最有研究价值的是：

- 查询时可以先按时间范围过滤文件

## 每条 Index Entry 的结构

每条记录固定 20 字节：

- `keyHash`：4B
- `phyOffset`：8B
- `timeDiff`：4B
- `prevIndexPos`：4B

它和 ConsumeQueue 一样都是 20 字节，但用途完全不同：

- ConsumeQueue 做顺序消费索引
- IndexFile 做哈希链式查找

## 为什么要存 `timeDiff`

这里不是直接存完整时间戳，而是：

- 相对于 `beginTimestamp` 的秒级差值

好处是：

- 节省空间
- 仍能支持时间范围查询

所以 IndexFile 既是 key 索引，也是一个带时间窗口的索引文件。

## 链式冲突处理怎么工作

`putKey(...)` 的核心逻辑是：

1. [[29-ConsumeQueue设计]]`key.hashCode()`
2. [[30-ReputMessageService异步分发]]落到某个 slot
3. 读出 slot 当前指向的旧 entry
4. 写入新 entry，并把 `prevIndexPos` 指向旧 entry
5. slot 头指针改成新 entry

这其实就是典型的头插法单链表。

所以冲突处理不是开放寻址，而是：

- slot 指向最新 entry
- entry 串成回溯链

## 查询为什么从后往前扫文件

`IndexService.queryOffset(...)` 会从最新 IndexFile 往前遍历。

原因是：

- 更可能命中最近消息
- 时间窗口裁剪更高效

而且一旦遇到：

- `f.getBeginTimestamp() < begin`

就可以提前停止。

这说明它的优化重点是：

- 最近数据优先
- 时间窗口快速剪枝

## 5.5.0 的索引类型比旧认知更多

旧理解通常只记得：

- uniqKey
- 业务 key

但 5.5.0 的 `IndexService.buildIndex(...)` 里还支持：

- tag 索引

构造方式大致是：

- 普通 key：`topic#key`
- tag 索引：`topic#TAG#tag`

这说明 IndexFile 在 5.5.0 已经不只是“历史消息查询附属工具”，而是承担了更多查询辅助语义。

## 事务消息为什么也会进索引

`buildIndex(...)` 对事务类型的处理不是简单跳过所有事务消息，而是：

- `NOT_TYPE`
- `PREPARED_TYPE`
- `COMMIT_TYPE`

可以继续走

- `ROLLBACK_TYPE`

直接返回

所以研究事务消息时，如果你发现索引行为和“最终可见消息”不完全一致，要先回看这一层。

## IndexFile 不是无限增长的全局真相

它会随着 CommitLog 最小物理位点推进而删除旧文件。

也就是说：

- 物理消息删了
- 老索引也会被裁掉

所以按 key 查消息的能力也只在当前保留窗口内成立。

## 和 ConsumeQueue 的真实区别

### ConsumeQueue

- 面向消费主链
- 按 topic-queue 顺序组织
- O(1) 逻辑 offset 定位

### IndexFile

- 面向按 key 查询
- 按哈希组织
- 平均快，但存在冲突链回溯

它们都依赖 CommitLog，但解决的是不同问题。

## 这篇最值得记住的边界

- IndexFile 不是主消费索引。
- 它是辅助查询索引。
- 20 字节结构相同，不代表语义相同。
- 历史 key 查询也受物理保留窗口约束。

## 和后续笔记的关系

- [[37-消息追踪Trace]] 里很多 trace 查询思路会借助 key / topic 维度理解。
- [[33-消息存储清理与回溯边界]] 会解释它为什么也会被裁剪。

## 建议连读

1. [[29-ConsumeQueue设计]]
2. [[30-ReputMessageService异步分发]]
3. [[33-消息存储清理与回溯边界]]