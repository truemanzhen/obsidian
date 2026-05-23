# SimpleConsumer 与 ReceiptHandle 语义

> 研究定位：把 5.x 的 gRPC `SIMPLE_CONSUMER` 拆成“客户端类型、负载均衡、ReceiptHandle、Ack/续期”四个层次。
> 关键源码：`common/src/main/java/org/apache/rocketmq/common/consumer/ReceiptHandle.java`、`proxy/grpc/v2/client/ClientActivity.java`、`proxy/grpc/v2/consumer/AckMessageActivity.java`、`ChangeInvisibleDurationActivity.java`、`ReceiveMessageActivity.java`
> 阅读建议：先看本篇，再接 [[53-5.x消费模型与负载均衡]]、[[48-Pop消费模式详解]]、[[49-PopRevive机制]]。

## 先给结论

- `SIMPLE_CONSUMER` 是 gRPC / Proxy 侧的客户端类型。
- 它不是经典 Java Client 的 `DefaultLitePullConsumer`。
- 这条链路的核心不是 queue offset，而是 `ReceiptHandle`。

## 客户端类型是怎么分出来的

```java
case PUSH_CONSUMER:
case LITE_PUSH_CONSUMER:
case SIMPLE_CONSUMER: {
    validateConsumerGroup(request.getGroup());
    ...
}
```

```java
protected ConsumeType buildConsumeType(ClientType clientType) {
    switch (clientType) {
        case SIMPLE_CONSUMER:
            return ConsumeType.CONSUME_ACTIVELY;
        case PUSH_CONSUMER:
        case LITE_PUSH_CONSUMER:
            return ConsumeType.CONSUME_PASSIVELY;
        default:
            throw new IllegalArgumentException(...);
    }
}
```

## ReceiptHandle 是什么

```java
public class ReceiptHandle {
    private final long startOffset;
    private final long retrieveTime;
    private final long invisibleTime;
    private final long nextVisibleTime;
    private final int reviveQueueId;
    private final String topicType;
    private final String brokerName;
    private final int queueId;
    private final long offset;
    private final long commitLogOffset;
}
```

它不是一个纯 token，而是携带了：

- 可见性窗口
- broker/queue 定位信息
- pop/revive 相关元数据

## ack 的本质

### gRPC 入口

```java
String handleString = getHandleString(ctx, group, request, ackMessageEntry);
handleMessageList.add(new ReceiptHandleMessage(ReceiptHandle.decode(handleString), ackMessageEntry.getMessageId()));
return this.messagingProcessor.batchAckMessage(...);
```

### Proxy 处理

```java
ackMessageRequestHeader.setExtraInfo(handle.getReceiptHandle());
ackMessageRequestHeader.setOffset(handle.getOffset());
...
future = this.serviceManager.getMessageService().ackMessage(...)
```

### 语义结论

- ack 不是“提交 offset”。
- ack 是把对应 receipt handle 从处理中状态移除。

## 续期怎么做

```java
MessageReceiptHandle messageReceiptHandle =
    messagingProcessor.removeReceiptHandle(...);
if (messageReceiptHandle != null) {
    receiptHandle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
}
return this.messagingProcessor.changeInvisibleTime(...);
```

续期时，Proxy 会先尝试拿到最新 handle，再调用 broker 变更不可见时间。

## ReceiveMessage 时如何把 handle 存起来

```java
String receiptHandle = messageExt.getProperty(MessageConst.PROPERTY_POP_CK);
if (receiptHandle != null) {
    MessageReceiptHandle messageReceiptHandle =
        new MessageReceiptHandle(group, topic, messageExt.getQueueId(), receiptHandle, messageExt.getMsgId(),
            messageExt.getQueueOffset(), messageExt.getReconsumeTimes());
    messagingProcessor.addReceiptHandle(ctx, clientChannel, group, messageExt.getMsgId(), messageReceiptHandle);
}
```

## QueryAssignment 为什么重要

```java
QueryAssignmentRequestBody requestBody = new QueryAssignmentRequestBody();
requestBody.setTopic(topic);
requestBody.setConsumerGroup(consumerGroup);
requestBody.setClientId(clientId);
requestBody.setMessageModel(messageModel);
requestBody.setStrategyName(strategyName);
```

这说明 5.x 的负载均衡已经不只是客户端本地 `RebalanceImpl`。

## 研究时要记住

- `SIMPLE_CONSUMER` 和 `POP` / `Ack` / `ChangeInvisibleTime` 是同一条语义链。
- `ReceiptHandle` 是这条链的状态载体。
- `QueryAssignment` 是 5.x 服务端参与分配的入口之一。