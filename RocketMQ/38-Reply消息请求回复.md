# Reply 消息（请求-回复模式）

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/processor/ReplyMessageProcessor.java`

## 🏗️ 什么是 Reply 消息

Reply 消息实现了**请求-回复（Request-Reply）**语义，类似于 RPC 调用：
- 生产者发送请求消息
- 消费者处理后发送回复消息
- 生产者收到回复

```
Producer                    Broker                     Consumer
   │                          │                           │
   │ SEND_REPLY_MESSAGE       │                           │
   │ ─────────────────────→   │                           │
   │                          │  PUSH_REPLY_TO_CLIENT     │
   │                          │  ─────────────────────→   │
   │                          │                           │ 处理请求
   │                          │                           │
   │                          │  SEND_REPLY_MESSAGE       │
   │                          │  ←─────────────────────   │
   │  收到回复                 │                           │
   │ ←─────────────────────   │                           │
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                   Reply 消息流程                          │
├─────────────────────────────────────────────────────────┤
│  1. Producer 发送请求消息                                │
│     └── RequestCode: SEND_REPLY_MESSAGE                 │
│         └── 设置 replyToClientId, correlationId         │
├─────────────────────────────────────────────────────────┤
│  2. Broker 处理                                         │
│     ├── ReplyMessageProcessor.processRequest()          │
│     ├── 存储消息到 CommitLog                             │
│     └── PushReplyResult → 推送给目标 Consumer            │
├─────────────────────────────────────────────────────────┤
│  3. Consumer 收到请求                                   │
│     ├── 处理业务逻辑                                     │
│     └── 发送回复消息                                     │
│         └── RequestCode: SEND_REPLY_MESSAGE             │
├─────────────────────────────────────────────────────────┤
│  4. Broker 将回复推送给 Producer                         │
│     └── PUSH_REPLY_MESSAGE_TO_CLIENT                    │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `ReplyMessageProcessor` | Broker 侧处理 Reply 消息的处理器 |
| `ReplyMessageRequestHeader` | Reply 消息请求头 |
| `SendMessageRequestHeader` | 通用发送消息请求头（包含 replyToClientId） |

## 📝 关键字段

```java
// SendMessageRequestHeader 中的 Reply 相关字段
private String producerGroup;       // 生产者组
private String replyToClientId;     // 回复目标客户端 ID
private long correlationId;         // 关联 ID（匹配请求和回复）
private boolean storeReplyMessage;  // 是否存储 Reply 消息
```

## 🔄 处理流程

### ReplyMessageProcessor.processRequest()

```java
@Override
public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
    // 1. 解析请求头
    SendMessageRequestHeader requestHeader = parseRequestHeader(request);

    // 2. 构建 Trace 上下文
    SendMessageContext mqtraceContext = buildMsgContext(ctx, requestHeader, request);

    // 3. 处理 Reply 消息
    RemotingCommand response = processReplyMessageRequest(ctx, request, mqtraceContext, requestHeader);

    return response;
}
```

### processReplyMessageRequest()

```java
private RemotingCommand processReplyMessageRequest(ChannelHandlerContext ctx,
    RemotingCommand request, SendMessageContext mqtraceContext,
    SendMessageRequestHeader requestHeader) {

    // 1. 构建消息对象
    MessageExtBrokerInner msgInner = buildMessage(requestHeader, request.getBody());

    // 2. 设置 Reply 特有属性
    msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes());

    // 3. 推送给目标客户端（Producer）
    PushReplyResult pushResult = pushReplyMessage(ctx, requestHeader, msgInner);

    // 4. 可选：存储 Reply 消息
    if (brokerController.getBrokerConfig().isStoreReplyMessageEnable()) {
        PutMessageResult putResult = brokerController.getMessageStore().putMessage(msgInner);
        // 处理存储结果
    }

    // 5. 返回响应
    return response;
}
```

### pushReplyMessage()

```java
private PushReplyResult pushReplyMessage(ChannelHandlerContext ctx,
    SendMessageRequestHeader requestHeader, MessageExtBrokerInner msgInner) {

    // 1. 构建 Reply 请求头
    ReplyMessageRequestHeader replyHeader = new ReplyMessageRequestHeader();
    replyHeader.setTopic(msgInner.getTopic());
    replyHeader.setQueueId(msgInner.getQueueId());
    replyHeader.setStoreTimestamp(msgInner.getStoreTimestamp());

    // 2. 构建请求命令
    RemotingCommand request = RemotingCommand.createRequestCommand(
        RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, replyHeader);
    request.setBody(msgInner.getBody());

    // 3. 推送给目标 Producer
    // 通过 replyToClientId 找到目标客户端 Channel
    Channel targetChannel = findChannelByClientId(requestHeader.getReplyToClientId());

    // 4. 发送
    targetChannel.writeAndFlush(request);

    return new PushReplyResult(PushReplyStatus.SUCCESS);
}
```

## 📋 使用示例

### Producer 端

```java
DefaultMQProducer producer = new DefaultMQProducer("ProducerGroup");
producer.start();

// 创建请求消息
Message msg = new Message("RequestTopic", "Hello RocketMQ".getBytes());

// 发送请求并等待回复
RequestResponseFuture future = producer.request(msg, 3000);
// 或异步方式
producer.request(msg, new RequestCallback() {
    @Override
    public void onSuccess(Message responseMsg) {
        System.out.println("收到回复: " + new String(responseMsg.getBody()));
    }
    @Override
    public void onException(Throwable e) {
        e.printStackTrace();
    }
}, 3000);
```

### Consumer 端

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroup");
consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
    for (MessageExt msg : msgs) {
        // 处理请求
        String response = processRequest(msg);

        // 发送回复
        Message replyMsg = new Message(msg.getProperty(MessageConst.PROPERTY_REPLY_TO_CLIENT_ID),
            response.getBytes());
        producer.send(replyMsg);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
```

## 💡 设计亮点

1. **透明推送**：Broker 主动将回复推送给 Producer，无需轮询
2. **correlationId 匹配**：通过关联 ID 将请求和回复配对
3. **可选存储**：`storeReplyMessageEnable` 控制是否持久化 Reply 消息
4. **超时处理**：Producer 侧有超时机制，避免永久等待
5. **Trace 支持**：Reply 消息也有完整的 Trace 追踪

## ⚠️ 注意事项

1. **Reply 消息不走正常消费流程**：直接推送给 Producer
2. **Producer 必须在线**：如果 Producer 离线，回复会丢失
3. **超时时间**：合理设置超时，避免 Producer 长期阻塞
4. **Reply Topic**：建议使用独立的 Topic 作为 Reply 通道

## 🔗 与其他模块的关系

- **SendMessageProcessor**：ReplyMessageProcessor 继承自 AbstractSendMessageProcessor
- **Client**：Producer 使用 `request()` 方法，Consumer 发送回复消息
- **Trace**：Reply 消息有专门的 TraceType.RECALL

---

#核心类 #设计模式
