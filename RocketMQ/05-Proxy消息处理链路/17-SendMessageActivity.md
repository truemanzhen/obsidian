# SendMessageActivity — 生产者发送链路

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/producer/SendMessageActivity.java`
> 🏷️ #核心类

## 📐 发送流程

```
SendMessageRequest (gRPC)
    │
    ▼
sendMessage(ctx, request)
    │
    ├─ ① 参数校验
    │     request.getMessagesCount() > 0
    │
    ├─ ② 提取消息
    │     messageList = request.getMessagesList()
    │     topic = message.getTopic()
    │     validateTopic(topic)
    │
    ├─ ③ 构建内部消息对象
    │     buildMessage(ctx, messageList, topic)
    │     buildSysFlag(message)
    │
    ├─ ④ 选择目标 Queue
    │     SendMessageQueueSelector
    │
    ├─ ⑤ 调用 MessagingProcessor
    │     messagingProcessor.sendMessage(ctx, selector, topic, sysFlag, messages)
    │
    └─ ⑥ 转换响应
          convertToSendMessageResponse(ctx, request, resultList)
```

## 🔍 关键步骤详解

### 1. 消息转换 — Protobuf → 内部 Message

```java
protected Message buildMessage(ProxyContext context,
    apache.rocketmq.v2.Message protoMessage, String producerGroup) {

    Message messageExt = new Message();
    messageExt.setTopic(topicName);
    messageExt.setBody(protoMessage.getBody().toByteArray());

    // 构建属性（最关键的一步）
    Map<String, String> messageProperty = buildMessageProperty(context, protoMessage, producerGroup);
    MessageAccessor.setProperties(messageExt, messageProperty);

    return messageExt;
}
```

### 2. 属性构建 — buildMessageProperty()

```java
protected Map<String, String> buildMessageProperty(...) {
    // ① 用户属性 → 校验数量、Key/Value 不含控制字符、不与系统属性冲突
    for (Map.Entry<String, String> userPropertiesEntry : userProperties.entrySet()) {
        if (MessageConst.STRING_HASH_SET.contains(userPropertiesEntry.getKey())) {
            throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, ...);
        }
    }

    // ② Tag
    messageWithHeader.setTags(tag);

    // ③ Keys（业务 Key，支持多个）
    messageWithHeader.setKeys(keysList);

    // ④ UniqueKey（消息唯一 ID）
    MessageAccessor.putProperty(messageWithHeader,
        MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);

    // ⑤ 事务属性
    if (messageType.equals(MessageType.TRANSACTION)) {
        MessageAccessor.putProperty(messageWithHeader,
            MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
    }

    // ⑥ 延迟消息
    fillDelayMessageProperty(message, messageWithHeader);

    // ⑦ 消息组（FIFO 消息的顺序保证）
    MessageAccessor.putProperty(messageWithHeader,
        MessageConst.PROPERTY_SHARDING_KEY, messageGroup);

    // ⑧ 追踪上下文
    MessageAccessor.putProperty(messageWithHeader,
        MessageConst.PROPERTY_TRACE_CONTEXT, traceContext);

    // ⑨ Producer Group
    MessageAccessor.putProperty(messageWithHeader,
        MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);

    return messageWithHeader.getProperties();
}
```

> [!important] 属性校验链
> Proxy 在发送前做了大量校验，这些在 4.x 中通常由 Broker 做：
> - 用户属性数量限制
> - 属性 Key 不能与系统属性冲突
> - 控制字符检查
> - 消息体大小检查
> - 延迟时间范围检查
> - 事务恢复时间检查
>
> 这是 Proxy 层**防御性编程**的体现。

### 3. Queue 选择 — SendMessageQueueSelector

```java
protected static class SendMessageQueueSelector implements QueueSelector {

    @Override
    public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
        apache.rocketmq.v2.Message message = request.getMessages(0);
        String shardingKey = message.getSystemProperties().getMessageGroup();

        if (StringUtils.isNotEmpty(shardingKey)) {
            // 有 shardingKey → 一致性 Hash 选择 Queue
            // 保证相同 shardingKey 的消息发到同一个 Queue（FIFO 语义）
            List<AddressableMessageQueue> writeQueues = messageQueueView.getWriteSelector().getQueues();
            int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
            targetMessageQueue = writeQueues.get(bucket);
        } else {
            // 无 shardingKey → 轮询选择 Queue
            targetMessageQueue = messageQueueView.getWriteSelector().selectOneByPipeline(false);
        }
        return targetMessageQueue;
    }
}
```

> [!tip] 一致性 Hash 的好处
> 用 `consistentHash` 而不是简单的 `hashCode % size`：
> - 当 Queue 数量变化时（如 Broker 扩容），只有少量消息的目标 Queue 会变化
> - 保证 FIFO 消息的顺序性不被打破

### 4. 响应转换 — SendStatus → gRPC Code

```java
protected SendMessageResponse convertToSendMessageResponse(...) {
    for (SendResult result : resultList) {
        switch (result.getSendStatus()) {
            case SEND_OK:
                // 成功
                resultEntry = SendResultEntry.newBuilder()
                    .setStatus(Code.OK)
                    .setOffset(result.getQueueOffset())
                    .setMessageId(result.getMsgId())
                    .build();
                break;
            case FLUSH_DISK_TIMEOUT:
                // 刷盘超时
                resultEntry = ... Code.MASTER_PERSISTENCE_TIMEOUT ...;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                // 同步从节点超时
                resultEntry = ... Code.SLAVE_PERSISTENCE_TIMEOUT ...;
                break;
            case SLAVE_NOT_AVAILABLE:
                // 从节点不可用
                resultEntry = ... Code.HA_NOT_AVAILABLE ...;
                break;
        }
    }
}
```

## 📊 发送链路完整时序

```
Client    GrpcApp    Pipeline    Thread    SendActivity    MsgProcessor    Broker
  │         │          │          │           │               │              │
  │ Send    │          │          │           │               │              │
  │────────→│          │          │           │               │              │
  │         │ Auth     │          │           │               │              │
  │         │──────────│          │           │               │              │
  │         │ submit   │          │           │               │              │
  │         │─────────────────────│           │               │              │
  │         │          │          │ send      │               │              │
  │         │          │          │──────────→│               │              │
  │         │          │          │           │ buildMessage  │              │
  │         │          │          │           │ selectQueue   │              │
  │         │          │          │           │──────────────→│              │
  │         │          │          │           │               │ putMessage   │
  │         │          │          │           │               │─────────────→│
  │         │          │          │           │               │   Result     │
  │         │          │          │           │               │←─────────────│
  │         │          │          │           │ Response      │              │
  │         │          │          │           │←──────────────│              │
  │         │          │          │ Result    │               │              │
  │         │          │          │←──────────│               │              │
  │         │          │ Result   │           │               │              │
  │         │←────────────────────│           │               │              │
  │ Result  │          │          │           │               │              │
  │←────────│          │          │           │               │              │
```

## 💡 设计亮点

### 1. 防御性校验前置

Proxy 层做了大量校验（消息体大小、属性数量、控制字符等），
避免无效请求到达 Broker，减轻 Broker 压力。

### 2. 一致性 Hash 保证顺序

FIFO 消息通过 `shardingKey + consistentHash` 保证同组消息发到同一 Queue。

### 3. 全链路异步

`CompletableFuture` 贯穿全链路，发送线程不需要等待 Broker 响应。

---

> ⏭️ 下一篇：[[18-ReceiveMessageActivity]] — 消费者 Pop 消费链路
