# 消息撤回 Recall 设计

> 研究定位：把 `recallMessage` 这条链路从“一个 Producer API”拆成“句柄、Broker 校验、定时消息复用、Proxy 转发”四层。
> 关键源码：`common/src/main/java/org/apache/rocketmq/common/producer/RecallMessageHandle.java`、`client/.../DefaultMQProducerImpl.java`、`broker/.../RecallMessageProcessor.java`、`proxy/.../ProducerProcessor.java`
> 阅读建议：先看本篇，再接 [[34-延迟消息设计]] 和 [[37-消息追踪Trace]]。

## 先给结论

- Recall 不是任意消息都支持。
- 源码注释已经写明：当前只支持 delay message。
- 客户端拿到的 `recallHandle` 不是随机 token，而是 Base64 编码的结构化句柄。

## 句柄长什么样

```java
// version topic brokerName timestamp messageId
public static String buildHandle(String topic, String brokerName, String timestampStr, String messageId) {
    String rawString = String.join(SEPARATOR, VERSION_1, topic, brokerName, timestampStr, messageId);
    return Base64.getUrlEncoder().encodeToString(rawString.getBytes(UTF_8));
}
```

```java
public static RecallMessageHandle decodeHandle(String handle) throws DecoderException {
    ...
    String[] items = rawString.split(SEPARATOR);
    if (!VERSION_1.equals(items[0]) || items.length < 5) {
        throw new DecoderException("recall handle is invalid");
    }
    return new HandleV1(items[1], items[2], items[3], items[4]);
}
```

## 发送时如何生成 recallHandle

`SendMessageProcessor` 在发送成功后附带生成：

```java
String timestampStr = msg.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS);
String realTopic = msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
if (timestampStr != null && realTopic != null && !realTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
    timestampStr = String.valueOf(Long.parseLong(timestampStr) + 1);
    String recallHandle = RecallMessageHandle.HandleV1.buildHandle(
        realTopic,
        brokerController.getBrokerConfig().getBrokerName(),
        timestampStr,
        MessageClientIDSetter.getUniqID(msg));
    responseHeader.setRecallHandle(recallHandle);
}
```

结论：

- recallHandle 是发送结果的一部分。
- 它和 delay message 的时间字段强绑定。

## 客户端怎么发起撤回

```java
public String recallMessage(String topic, String recallHandle) {
    ...
    handleEntity = (RecallMessageHandle.HandleV1) RecallMessageHandle.decodeHandle(recallHandle);
    ...
    requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
    requestHeader.setTopic(topic);
    requestHeader.setRecallHandle(recallHandle);
    requestHeader.setBrokerName(handleEntity.getBrokerName());
    return this.mQClientFactory.getMQClientAPIImpl().recallMessage(...);
}
```

## Broker 端做了什么校验

```java
if (!brokerController.getBrokerConfig().isRecallMessageEnable()) ...
if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) ...
if (!PermName.isWriteable(...) && !isAllowRecallWhenBrokerNotWriteable()) ...
if (null == topicConfig) ...
if (!requestHeader.getTopic().equals(handle.getTopic())) ...
if (!brokerController.getBrokerConfig().getBrokerName().equals(handle.getBrokerName())) ...
if (timeLeft <= 0 || timeLeft >= timerMaxDelaySec * 1000L) ...
```

## Broker 实际写入了什么

```java
msgInner.setTopic(handle.getTopic());
msgInner.setBody("0".getBytes(StandardCharsets.UTF_8));
msgInner.setTags(RECALL_MESSAGE_TAG);
MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TIMER_DEL_UNIQKEY, ...);
MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(handle.getTimestampStr()));
MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_PRODUCER_GROUP, requestHeader.getProducerGroup());
```

所以 Recall 的本质更接近：

- 用 delay/timer 语义重建一条系统消息
- 再依赖 timer 链路完成后续处理

## Proxy 侧怎么转发

```java
if (ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()) {
    TopicMessageType messageType = serviceManager.getMetadataService().getTopicMessageType(ctx, topic);
    topicMessageTypeValidator.validate(messageType, TopicMessageType.DELAY);
}
```

Proxy 还会把请求转给 `RecallMessageProcessor` 对应 broker。

## 研究时要注意

- Recall 不是“用户随便删消息”。
- 它依赖 delay 消息能力。
- 句柄包含 topic、broker、时间戳、messageId，所以撤回有严格边界。
- 这条链路本身也会产生日志/trace 侧的研究价值。