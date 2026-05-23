# Reply 消息（请求-回复）

> 研究定位：把 RocketMQ 的 request-reply 从“像 RPC 一样使用”拆成请求消息属性、reply topic、Broker push reply 与客户端 future 匹配机制。
> 关键源码：`broker/.../ReplyMessageProcessor.java`、`client/.../DefaultMQProducerImpl.java`、`client/.../ClientRemotingProcessor.java`、`client/.../utils/MessageUtil.java`
> 阅读建议：先和普通发送、普通消费分开看；Reply 不是新 Topic 类型，而是一组消息属性、请求码和客户端回调闭环。

## 先给结论

- Request-Reply 的请求消息仍然是普通消息发送，只是会额外挂上 reply 相关属性。
- Reply 消息会走 `SEND_REPLY_MESSAGE` / `SEND_REPLY_MESSAGE_V2`，Broker 侧由 `ReplyMessageProcessor` 处理。
- Broker 真正主动 push 的是 reply 给请求方 producer，不是把请求消息主动 push 给 consumer。
- reply 是否落盘由 `BrokerConfig.storeReplyMessageEnable` 控制。
- 客户端最终靠 `PROPERTY_CORRELATION_ID` 把 reply 和本地等待中的 `RequestResponseFuture` 关联起来。

## 请求消息是怎样准备出来的

`DefaultMQProducerImpl.prepareSendRequest(...)` 会给请求消息补 3 个关键属性：

```java
MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));
```

也就是：

- `CORRELATION_ID`：请求与回复的关联键
- `MESSAGE_REPLY_TO_CLIENT`：告诉 Broker 回复应该推给哪个客户端
- `MESSAGE_TTL`：请求方愿意等待回复的超时时间

所以 request-reply 不是靠 `SendMessageRequestHeader` 多了几个 reply 字段实现的，而是靠 message property 驱动。

## consumer 回复时到底发的是什么

Consumer 侧并不是手写一条“普通消息回复到原 topic”。

推荐入口是：

```java
Message replyMessage = MessageUtil.createReplyMessage(requestMessage, body);
```

`MessageUtil.createReplyMessage(...)` 会：

1. 从请求消息取出 `PROPERTY_CLUSTER`
2. 取出 `PROPERTY_MESSAGE_REPLY_TO_CLIENT`
3. 取出 `PROPERTY_CORRELATION_ID`
4. 把 topic 设成 `MixAll.getReplyTopic(cluster)`
5. 写入 `PROPERTY_MESSAGE_TYPE=reply`
6. 把 `CORRELATION_ID` 和 `MESSAGE_REPLY_TO_CLIENT` 原样带回去

所以 Reply 并不是“consumer 回复到原请求 topic”，而是发到 reply topic，并靠属性完成路由与匹配。

## Broker 侧处理主线

### 请求码分流

`MQClientAPIImpl.sendMessage(...)` 会根据消息类型是否为 reply 决定请求码：

- 普通消息：`SEND_MESSAGE` / `SEND_MESSAGE_V2`
- Reply 消息：`SEND_REPLY_MESSAGE` / `SEND_REPLY_MESSAGE_V2`

Broker 启动时会把 reply 请求码注册到 `ReplyMessageProcessor`。

### `ReplyMessageProcessor.processReplyMessageRequest(...)`

主线可以概括成：

1. `msgCheck(...)`
2. 构造 `MessageExtBrokerInner`
3. 先 `pushReplyMessage(...)`
4. 再视配置决定是否 `putMessage(...)`

源码顺序上，push reply 是先发生的，存储 reply 是可选的附加动作。

### push 给谁

`pushReplyMessage(...)` 最关键的一句是：

```java
String senderId = msg.getProperties().get(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
Channel channel = brokerController.getProducerManager().findChannel(senderId);
```

这说明 reply 的目标是：

- 请求方 producer 对应的 clientId / channel

如果 channel 找不到，就会直接返回 push 失败。

所以这里最核心的边界是：

- 请求消息照常被 consumer 消费
- reply 消息由 Broker 定向推回 producer

## producer 端怎么收 reply

客户端接收入口是 `ClientRemotingProcessor.receiveReplyMessage(...)`。

它会：

1. 把 remoting 请求反序列化成 `MessageExt`
2. 从属性里取 `PROPERTY_CORRELATION_ID`
3. 查本地 `RequestFutureHolder` 里的等待表
4. 找到对应 `RequestResponseFuture` 后塞入 reply message
5. 同步请求就唤醒等待，异步请求就回调 `RequestCallback`

也就是：

- Broker 负责把 reply 推回客户端
- 客户端负责按 correlationId 交给正确的等待方

## 为什么这不是“真正的 RPC 框架”

虽然 API 形态很像 RPC，但从源码边界看它仍然是消息系统上的 request-reply 语义：

- 请求消息先作为 MQ 消息发送
- consumer 仍然按普通消费链路处理请求
- reply 再通过 Broker 定向推回 producer
- 超时、丢失、离线场景仍按消息系统约束处理

所以研究时不要把它想成强一致、强在线、端到端调用保证的 RPC 通道。

## `storeReplyMessageEnable` 的意义

`ReplyMessageProcessor` 里：

```java
if (brokerController.getBrokerConfig().isStoreReplyMessageEnable()) {
    brokerController.getMessageStore().putMessage(msgInner);
}
```

这说明 reply push 成功与否，和 reply 是否持久化是两个独立维度：

- push 用于把结果尽快送回请求方
- store 用于是否把 reply 作为消息持久化留痕

## 这篇里最容易写错的地方

- 不是 `SendMessageRequestHeader` 里直接多了 `replyToClientId` / `correlationId` 字段。
- 不是 Broker 主动把“请求”推给 consumer。
- 不是 Reply 会走消息撤回那条 trace 类型。

最后一点尤其要注意：

- `TraceType.Recall` 是消息撤回的 trace
- Reply 这条链路并没有单独的 `Reply` trace 枚举

## 和其他笔记的关系

- [[08-客户端Producer内部实现]]：看 request() 最终怎样落到 producer 发送链。
- [[25-Broker核心处理器]]：看 `ReplyMessageProcessor` 在 Broker 处理器表里的位置。
- [[39-消息撤回Recall设计]]：不要把 reply 和 recall 的 trace / 处理器混淆。

## 研究时要继续追问的问题

- producer 离线时，reply push 失败和 reply 落盘之间的行为组合是什么。
- reply topic 在路由和权限上与普通业务 topic 有哪些差异。
- request 超时后晚到的 reply，客户端会怎样处理未匹配消息。