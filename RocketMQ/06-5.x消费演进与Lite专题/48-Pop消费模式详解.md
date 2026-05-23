# Pop 消费模式详解

> 研究定位：把 POP 从“服务端调度的新消费方式”拆成 assignment、客户端执行路径、Broker 状态管理与重试闭环。
> 关键源码：`broker/.../PopMessageProcessor.java`、`broker/.../AckMessageProcessor.java`、`broker/.../ChangeInvisibleTimeProcessor.java`、`broker/.../pop/`、`client/.../RebalanceImpl.java`、`client/.../DefaultMQPushConsumerImpl.java`
> 阅读建议：先看 [[51-MessageRequestModeManager]] 和 [[47-长轮询机制详解]]，再回来读具体运行时细节。

## 先给结论

- `POP` 是 broker 请求模式，不是一个单独的 Java 客户端类型。
- 在经典 Java Client 里，POP 仍然落在 `DefaultMQPushConsumerImpl` 这条消费链上，不存在独立的 POP 客户端类型。
- 5.5.0 里的 POP 并没有把客户端调度全部抹掉；它仍然通过 assignment / `RebalanceImpl` 把 `PopRequest` 分发出去。
- 它真正改变的是“消费确认与重试模型”：从 offset 驱动，变成 `ack / invisibleTime / revive` 驱动。

## 最容易误解的点

### 1. 并不存在独立的 POP 客户端类型

旧资料里经常会写一个并不存在的独立 POP 客户端类。

5.5.0 源码里经典 Java Client 的 POP 路径是：

- `DefaultMQPushConsumerImpl`
- `ConsumeMessagePopConcurrentlyService` / `ConsumeMessagePopOrderlyService`
- `pullAPIWrapper.popAsync(...)`

也就是：

- 仍然是 PushConsumer 实现体承接 POP
- 只是在 assignment 结果里拿到 `POP` 模式后，切换到 `PopRequest` 路径

### 2. [[47-长轮询机制详解]]“不需要 Rebalance”是过度简化

`RebalanceImpl.getRebalanceResultFromBroker(...)` 会拿到 `MessageQueueAssignment` 集合；
`updateMessageQueueAssignment(...)` 会按 `assignment.getMode()` 分成：

- `mq2PushAssignment`
- `mq2PopAssignment`

然后：

- push assignment 生成 `PullRequest`
- pop assignment 生成 `PopRequest`

这说明 POP 并不是完全绕开 assignment，而是改变了 assignment 结果的消费语义。

### 3. “客户端无状态”也只能说一半

Broker 当然维护了更重的消息可见性与重试状态，但客户端侧仍然有：

- `PopProcessQueue`
- 等待 ack 的计数
- 本地限流
- 消费线程池

所以更准确的说法应是：

- POP 把“消息可见性、超时重试、重投闭环”更多收回到 Broker
- 但客户端并不是零状态执行器

## POP 在客户端怎么跑起来

### assignment 阶段

`QueryAssignmentProcessor` 返回的 assignment 带上 `mode`。

`RebalanceImpl.updateMessageQueueAssignment(...)` 遇到 `MessageRequestMode.POP` 时，会给对应 queue 创建：

```java
PopRequest popRequest = new PopRequest();
popRequest.setTopic(topic);
popRequest.setConsumerGroup(consumerGroup);
popRequest.setMessageQueue(mq);
popRequest.setPopProcessQueue(pq);
popRequest.setInitMode(getConsumeInitMode());
```

然后通过：

```java
dispatchPopPullRequest(popRequestList, 500);
```

进入客户端执行链。

### 执行阶段

`RebalancePushImpl.dispatchPopPullRequest(...)` 最终调用：

```java
defaultMQPushConsumerImpl.executePopPullRequestImmediately(...)
```

随后进入 `DefaultMQPushConsumerImpl.popMessage(...)`。

核心逻辑包括：

- 消费者暂停检查
- `popThresholdForQueue` 等本地流控
- 读取订阅表达式
- 调用 `pullAPIWrapper.popAsync(...)`
- 成功后进入 `consumeMessagePopService.submitPopConsumeRequest(...)`

这说明经典客户端 POP 依旧保留了：

- 本地调度
- 本地流控
- 本地消费线程池

## Broker 侧到底做了什么

### PopMessageProcessor / PopConsumerService

Broker 侧 POP 不再只是“按 offset 读一段消息返回”。

它需要同时处理：

- 正常 topic
- POP retry topic
- invisible time
- 检查点与确认信息
- revive 重投

这也是为什么 POP 的复杂度明显高于传统 pull。

### ACK 的语义

POP 的确认不是“更新消费 offset”，而是围绕检查点做确认。

对应入口：

- `AckMessageProcessor`
- `ChangeInvisibleTimeProcessor`

它们处理的是：

- ack 成功，代表这条消息生命周期结束
- changeInvisibleTime，代表延长不可见窗口

这与 Pull/Push 里的 offset 提交不是同一个维度。

## 为什么 POP 会影响 retry 语义

`RebalanceImpl.updateMessageQueueAssignment(...)` 里还有一段非常关键的逻辑：

- push 切到 pop 时，会取消普通 push 对 pop retry topic 的订阅
- pop 切回 push 时，会重新订阅 pop retry topic

这说明：

- POP 不是只改变拉取入口
- 它还会连带改变 retry topic 的订阅和消息流向

## `DefaultMQPushConsumerImpl.popMessage(...)` 的几个关键边界

### 1. 本地等待 ACK 数量会触发流控

```java
if (processQueue.getWaiAckMsgCount() > popThresholdForQueue) {
    ...
}
```

这说明 POP 并不是“Broker 管了就一定无限轻”；
客户端侧仍然需要控制未确认消息堆积。

### 2. [[47-长轮询机制详解]]仍然有订阅过滤

回调成功后会走 `processPopResult(...)`，里面还会：

- 按 tag 再做一次客户端过滤
- 执行 `FilterMessageHook`
- 对超出最大重试次数的消息直接 ack

所以 POP 返回到客户端后，仍然不是“完全无需本地处理的原始消息流”。

### 3. 拉取方式是 `popAsync(...)`

真正对 Broker 的请求是：

```java
pullAPIWrapper.popAsync(...)
```

这和 `pullKernelImpl(...)` 路径已经分开，说明 POP 在协议语义上确实是独立请求模式。

## 这篇和其他笔记的关系

### 和  的关系

- 那篇解决“Push 本质仍是 Pull”
- 本篇解决“POP 为什么不只是另一个 Push API”

### 和 [[49-PopRevive机制]] 的关系

- 本篇只把 revive 当成闭环的一环
- 真正的超时扫描、重投细节在 [[49-PopRevive机制]]

### 和 [[54-SimpleConsumer与ReceiptHandle语义]] 的关系

- 5.x proxy / gRPC 的 `SIMPLE_CONSUMER` 也建立在 ack / invisible time 模型上
- 但它暴露给用户的是 `ReceiptHandle`
- 经典 Java Client POP 与 gRPC SimpleConsumer 不是同一层 API

## 研究时要避免的错误说法

- “RocketMQ 5.5.0 有个独立的 POP 客户端类。”
- “POP 完全不需要 Rebalance。”
- “POP 就是 Broker 主动把消息推给客户端。”
- “POP 只是 Push 的另一个名字。”

这些说法都会掩盖真正有价值的源码边界。

## 建议继续阅读

1. [[51-MessageRequestModeManager]]
2. [[47-长轮询机制详解]]
3. [[49-PopRevive机制]]
4. [[54-SimpleConsumer与ReceiptHandle语义]]