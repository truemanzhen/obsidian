# FIFO 与顺序消息语义

> 研究定位：把 RocketMQ 5.5.0 里的“顺序消息”拆成三层：Topic/消息类型约束、发送侧分片路由、消费侧顺序保证。重点回答 classic orderly 和 POP orderly 到底是不是同一套机制。
> 关键源码：`proxy/.../SendMessageActivity.java`、`common/.../TopicMessageType.java`、`client/.../ConsumeMessageOrderlyService.java`、`client/.../RebalancePushImpl.java`、`broker/.../pop/orderly/QueueLevelConsumerManager.java`、`broker/.../processor/PopMessageProcessor.java`
> 阅读建议：不要把 FIFO 只理解成“同一个 key 落同一个 queue”。那只是发送侧第一步，消费侧还分 classic orderly 和 POP orderly 两条线。

## 先给结论

- 5.5.0 里“顺序消息”至少涉及三层语义：Topic 类型、发送路由、消费顺序保证。
- 发送侧 FIFO 的最直接触发条件是消息属性里有 `PROPERTY_SHARDING_KEY`，在 gRPC/Proxy 侧对应 `messageGroup`。
- Proxy 发送 FIFO 时会用一致性 Hash 把相同 `messageGroup` 映射到同一 write queue。
- 经典 Push 顺序消费依赖客户端 `MessageListenerOrderly`、Rebalance 锁和 Broker `LOCK_BATCH_MQ/UNLOCK_BATCH_MQ` 协议。
- POP 顺序消费则是另一条线：Broker 侧 `ConsumerOrderInfoManager` 维护队列级阻塞与提交推进，不再只靠经典 queue lock。

## 顺序语义先看三层

### 1. [[23-SendMessageActivity]]Topic / 消息类型层

RocketMQ 5.x 已经把 FIFO 显式放进 `TopicMessageType`：

- `FIFO`
- `MIXED` 中也可能允许 FIFO

也就是说 FIFO 不再只是“应用自己约定一个 sharding key”。

它还是：

- topic 属性
- 校验链
- 路由暴露语义

的一部分。

### 2. [[09-客户端Rebalance实现]]发送层

发送层回答的是：

- 同一 message group 的消息如何尽量进同一 queue

这条线最直接在 Proxy 的 `SendMessageActivity`。

### 3. [[25-Broker核心处理器]]消费层

消费层回答的是：

- 同一 queue 的消息如何只被一个执行上下文按顺序推进

而这层在 5.5.0 已经分成 classic orderly 和 POP orderly 两条实现。

## 发送侧 FIFO：`messageGroup -> PROPERTY_SHARDING_KEY`

Proxy/gRPC 发送时，如果系统属性里带 `messageGroup`：

```java
MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
```

这意味着在服务端语义里：

- `messageGroup` 最终会沉成 `__SHARDINGKEY`

Broker 侧也会据此识别消息类型：

```java
if (properties.containsKey(MessageConst.PROPERTY_SHARDING_KEY)) {
    sendMessageContext.setMsgType(MessageType.Order_Msg);
}
```

所以 FIFO 首先是消息属性层面的显式标记。

## 发送侧顺序保证只是“尽量同组同 queue”

`SendMessageActivity.SendMessageQueueSelector` 会在有 `shardingKey` 时走：

```java
int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
targetMessageQueue = writeQueues.get(bucket);
```

这段逻辑解决的是：

- 相同分组落同一 queue
- queue 数量变化时减少大规模重映射

但它只解决“发到哪”，还没有解决“怎么按顺序消费完”。

## 经典顺序消费：客户端锁 + Broker queue lock

经典 Push 顺序消费的入口是：

- 业务注册 `MessageListenerOrderly`
- `DefaultMQPushConsumerImpl` 选择 `ConsumeMessageOrderlyService`

`ConsumeMessageOrderlyService.start()` 在 clustering 下会周期性：

- `lockMQPeriodically()`

同时 Rebalance 移除 queue 时，`RebalancePushImpl.removeUnnecessaryMessageQueue(...)` 会：

1. [[23-SendMessageActivity]]先持久化 offset
2. [[09-客户端Rebalance实现]]再 `unlock(mq, true)`

客户端底层协议则走：

- `LOCK_BATCH_MQ`
- `UNLOCK_BATCH_MQ`

Broker 侧对应处理入口在：

- `AdminBrokerProcessor`

所以 classic orderly 的第一性原理是：

- 某个 queue 在集群消费里同一时刻只应被一个消费者实例持锁推进

## classic orderly 的边界

这套机制天然是：

- queue 级顺序
- clustering 下依赖 broker queue lock

它不是：

- 任意 topic 全局总顺序
- 任意 message group 自动独立并发顺序

因此“RocketMQ 顺序消息”在 classic client 语境下，更接近：

- 把顺序约束收缩到 queue

## POP orderly 是另一套实现，不是 classic lock 的翻版

POP 顺序消费时，Broker 会在 `PopMessageProcessor` 里先做：

```java
if (brokerController.getConsumerOrderInfoManager().checkBlock(...)) {
    future.complete(restNum);
    return future;
}
```

成功取到消息后，再更新：

```java
brokerController.getConsumerOrderInfoManager().update(...);
brokerController.getConsumerOffsetManager().commitOffset(...);
```

后续 ack 时再通过：

- `AckMessageProcessor`
- `ChangeInvisibleTimeProcessor`

推进 `commitAndNext(...)`、更新 next visible time。

这说明 POP orderly 的关键对象已经变成：

- `ConsumerOrderInfoManager`
- `OrderInfo`
- attemptId / invisibleTime / offsetList

而不只是 classic queue lock。

## `ConsumerOrderInfoManager` 真正在记录什么

`QueueLevelConsumerManager` 的主表是：

```java
ConcurrentHashMap<String/*topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> table
```

每个 `OrderInfo` 会记录：

- attemptId
- popTime
- invisibleTime
- offsetList
- offsetConsumedCount

这说明 POP orderly 的状态机本质上是：

- 某个 topic@group@queue 当前有一批被弹出的顺序消息
- 在这批消息被 ack/超时前，后续同队列消息要被阻塞

## classic orderly 和 POP orderly 的真正差别

| 维度 | classic orderly | POP orderly |
|---|---|---|
| 消费入口 | `PULL_MESSAGE` | `POP_MESSAGE` |
| 主状态 | `ProcessQueue` + Broker queue lock | `OrderInfo` + invisible window |
| 推进依据 | 本地消费完成 + offset 提交 | ack / changeInvisible / commitAndNext |
| 阻塞方式 | queue 持锁 | `checkBlock(...)` |
| 关键类 | `ConsumeMessageOrderlyService`、`RebalancePushImpl` | `PopMessageProcessor`、`ConsumerOrderInfoManager` |

所以把 POP orderly 讲成“只是换了个 pull 接口”是不对的。

## FIFO 研究里最常见的误区

- “有 shardingKey 就自动全局有序。”  
  实际只是在发送侧把同组消息路由到同一 queue。

- “顺序消费就是 Broker 保证的。”  
  classic orderly 明显依赖客户端服务和 rebalance 锁。

- “POP 顺序和 classic orderly 是一套实现。”  
  源码上已经分叉成两套状态管理。

- “顺序消息只和 Producer 有关。”  
  实际上发送、Rebalance、消费确认、阻塞策略都参与。

## 研究时建议继续连读

- [[23-SendMessageActivity]]

- [[49-PopRevive机制]]