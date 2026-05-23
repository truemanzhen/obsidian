# StaticTopic 逻辑队列映射

> 研究定位：把 StaticTopic 的“逻辑队列不变、物理队列可迁移”这条主线讲清楚，回答为什么 RocketMQ 5.5.0 要引入一套单独的 mapping 结构。
> 关键源码：`remoting/src/main/java/org/apache/rocketmq/remoting/protocol/statictopic/LogicQueueMappingItem.java`、`TopicQueueMappingDetail.java`、`TopicQueueMappingUtils.java`、`TopicRemappingDetailWrapper.java`
> 阅读建议：这篇要和 `topic route`、`lite topic`、`remap` 命令一起看，不能只把它当成“固定队列数”。

## 先给结论

- StaticTopic 的核心不是新建一种队列对象，而是给现有 `MessageQueue` 增加逻辑到物理的映射层。
- 逻辑队列对外稳定，物理队列可以在 broker 间迁移。
- `LogicQueueMappingItem` 是最核心的映射节点，记录了逻辑 offset 和物理 offset 的对齐关系。
- `TopicQueueMappingUtils` 负责 remap 计算、路由重写和最终一致的辅助逻辑。

## StaticTopic 解决的问题

普通 topic 的队列数量和 broker 分布一旦变化，会影响：

- 消费 offset 连续性
- 路由稳定性
- 客户端感知

StaticTopic 想解决的是：

- 队列数固定
- 逻辑队列号固定
- 底层物理 broker 可以迁移

所以它的核心不是“静态配置”，而是：

- 把“逻辑队列”从“物理队列”解耦

## `LogicQueueMappingItem` 是什么

它是静态 topic 的最小映射单元。

一般会记录：

- `bname`
- `queueId`
- `logicOffset`
- `startOffset`
- `endOffset`

这些字段的意义是：

- 这段逻辑队列在某个 broker / queue 上的有效区间是什么

## `TopicQueueMappingDetail` 的角色

它是 topic 级别的映射信息容器。

你可以把它理解成：

- 一个 topic 下面所有逻辑队列的映射总表

它负责描述：

- 哪些 broker 参与映射
- 哪些队列属于哪个逻辑队列
- remap 前后怎么对齐

## 为什么 remap 不是简单搬文件

StaticTopic 的迁移要保证：

- 逻辑 offset 连续
- 消费端可感知的位置不乱
- 双读 / 校验读能够兜底

所以 remap 往往要做的是：

- 重新计算映射
- 重写路由
- 维护历史 mapping item

而不是把 topic 下所有数据“挪个目录”那么简单。

## 5.5.0 里 static topic 和 lite topic 不是一回事

这两个名字容易混。

- StaticTopic：强调逻辑队列和物理队列映射可变
- LiteTopic：强调轻量消费/LMQ 相关语义

它们都涉及 topic 形态变化，但解决的问题不同。

## 路由重写为什么重要

客户端最终拿到的是 route data。

如果静态 topic 做了映射调整，但路由没有正确重写：

- 客户端会继续按旧物理认知发送/消费

所以 StaticTopic 的核心工作不仅是服务端存储，还包括：

- 路由暴露层同步更新

## 这篇最值得记住的点

- StaticTopic 的本质是逻辑队列与物理队列解耦。
- `LogicQueueMappingItem` 是最小事实单元。
- remap 的重点是连续性与可迁移性，不是简单搬迁。

## 和后续笔记的关系

-  会补 topic 语义约束。
- [[52-LiteMode与LiteTopic]] 会补 LiteTopic / LMQ 的另一条轻量消费线。

## 建议连读

1. [[04-Topic与消息类型约束]]
2. [[52-LiteMode与LiteTopic]]
3. [[53-5.x消费模型与负载均衡]]