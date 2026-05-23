# SendMessageActivity

> 研究定位：回答 Proxy/gRPC 发送入口到底做了哪些“前移到接入层”的工作，尤其是属性校验、消息类型转换和队列选择。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/producer/SendMessageActivity.java`

## 先给结论

- `SendMessageActivity` 不负责直接写 Broker，它负责把 gRPC `Message` 变成 RocketMQ 内部 `Message`。
- 它在 Proxy 层前移了大量校验：消息体、属性、tag、key、messageGroup、延迟时间、事务恢复时间。
- FIFO / LiteTopic / 延迟 / 事务 这些高级语义，都会在这里先沉成消息属性。
- 顶层发送状态不是简单单值，批量发送时可能聚合成 `MULTIPLE_RESULTS`。

## 源码骨架

```java
future = this.messagingProcessor.sendMessage(
    ctx,
    new SendMessageQueueSelector(request),
    topic.getName(),
    buildSysFlag(message),
    buildMessage(ctx, request.getMessagesList(), topic)
).thenApply(result -> convertToSendMessageResponse(ctx, request, result));
```

## 主链路

### 1. 基础校验

入口先做三件事：

- `messagesCount > 0`
- 同一批消息必须同 topic
- topic 合法

这里已经把最粗的非法请求挡在 Proxy 层了。

### 2. protobuf -> 内部消息

`buildMessage(...)` 会把：

- body
- topic
- properties

组装成 `org.apache.rocketmq.common.message.Message`。

### 3. `sysFlag` 生成

这里只处理两类位：

- `Encoding.GZIP` 对应压缩标记
- `MessageType.TRANSACTION` 对应事务 prepared 标记

### 4. 队列选择

`SendMessageQueueSelector` 优先看：

- `messageGroup`
- 如果没有，再退到 `liteTopic`

只在单条消息时才会使用这条分组键路线。

### 5. 结果聚合

`convertToSendMessageResponse(...)` 会把每个 `SendResult` 转成 `SendResultEntry`，然后再决定顶层状态：

- 全部成功：`OK`
- 混合状态：`MULTIPLE_RESULTS`
- 不同失败：映射到 `MASTER_PERSISTENCE_TIMEOUT`、`SLAVE_PERSISTENCE_TIMEOUT`、`HA_NOT_AVAILABLE` 等

## 真正被前移到 Proxy 层的语义

### 用户属性

这里会检查：

- 用户属性数量
- key 是否和系统属性冲突
- key/value 是否含控制字符
- 总大小是否超限

### 消息唯一标识

`messageId` 不能为空，并会写到：

- `PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX`

### 事务消息

事务消息会显式写入：

- `PROPERTY_TRANSACTION_PREPARED`
- 可选的 `PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS`

### 延迟消息

如果有投递时间，会写入：

- `PROPERTY_TIMER_DELIVER_MS`
- 如果启用 delay level，还会写 `PROPERTY_DELAY_TIME_LEVEL`

### FIFO / 顺序消息

如果有 `messageGroup`，会写入：

- `PROPERTY_SHARDING_KEY`

后续队列选择也会依据它做一致性哈希。

### LiteTopic

如果有 `liteTopic`，也会先校验再落到属性里，并且在某些场景下参与队列选择。

## 队列选择的真实语义

```java
if (StringUtils.isNotEmpty(shardingKey)) {
    int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
    targetMessageQueue = writeQueues.get(bucket);
} else {
    targetMessageQueue = messageQueueView.getWriteSelector().selectOneByPipeline(false);
}
```

这段代码说明：

- FIFO 不是 Broker 后补的概念
- Proxy 在发送前就参与了顺序路由

## 容易误解的点

- `SendMessageActivity` 不是 producer SDK 的发送逻辑，它是 Proxy 侧发送入口。
- 它不决定最终怎么落盘，只决定“发给谁、带什么属性、怎么解释结果”。
- FIFO 的第一步是分组键路由，不是消费端顺序保证。
- 批量发送不是“多条消息一起发就行”，它仍要求 topic 一致并逐条构造内部消息。

## 关联阅读

- [[24-ReceiveMessageActivity]]