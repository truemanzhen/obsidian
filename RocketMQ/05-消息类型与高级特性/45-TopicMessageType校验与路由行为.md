# TopicMessageType 校验与路由行为

> 研究定位：把 `TopicMessageType` 从“枚举定义”拉到运行时行为层面。
> 关键源码：`ProxyConfig.enableTopicMessageTypeCheck`、`DefaultTopicMessageTypeValidator`、`ProducerProcessor`、`RouteActivity`、`GrpcConverter`
> 阅读建议：把这篇和 [[04-Topic与消息类型约束]] 对照看，一篇负责“抽象本身”，一篇负责“抽象怎么生效”。

## 先给结论

- `TopicMessageType` 不只是元数据。
- 它直接参与发送校验、撤回校验、路由暴露和 gRPC 消息类型转换。
- `MIXED` 不是“所有类型全开”，而是一个有限放宽策略。

## 发送校验链路

Proxy 侧核心路径是：

```text
Message properties
   ↓
TopicMessageType.parseFromMessageProperty(...)
   ↓
MetadataService.getTopicMessageType(...)
   ↓
DefaultTopicMessageTypeValidator.validate(expected, actual)
```

验证器源码很短，但很关键：

```java
if (actualType.equals(TopicMessageType.UNSPECIFIED)
        || !actualType.equals(expectedType) && !expectedType.equals(TopicMessageType.MIXED)) {
    throw new ProxyException(...);
}
```

这意味着：

- `actual == UNSPECIFIED` 直接失败。
- 只有 `expected == MIXED` 时，才允许实际类型和配置类型不完全一致。
- `MIXED` 不是取消校验，而是显式放宽。

## Recall 也走这个约束

`ProducerProcessor.recallMessage(...)` 不是简单跳过类型系统，而是强校验：

```java
TopicMessageType messageType = serviceManager.getMetadataService().getTopicMessageType(ctx, topic);
topicMessageTypeValidator.validate(messageType, TopicMessageType.DELAY);
```

所以 recall 的 Topic 契约是：

- topic 必须是 `DELAY`
- 不是只看 recallHandle 是否合法

## 路由暴露的行为

`RouteActivity.parseTopicMessageType(...)` 会把 Topic 类型转成 gRPC `acceptMessageTypes`：

```java
case NORMAL:
    return Collections.singletonList(MessageType.NORMAL);
case FIFO:
    return Collections.singletonList(MessageType.FIFO);
case LITE:
    return Collections.singletonList(MessageType.LITE);
case TRANSACTION:
    return Collections.singletonList(MessageType.TRANSACTION);
case DELAY:
    return Collections.singletonList(MessageType.DELAY);
case PRIORITY:
    return Collections.singletonList(MessageType.PRIORITY);
case MIXED:
    return Arrays.asList(MessageType.NORMAL, MessageType.FIFO, MessageType.DELAY, MessageType.TRANSACTION);
default:
    return Collections.singletonList(MessageType.MESSAGE_TYPE_UNSPECIFIED);
```

最重要的两个点：

- `MIXED` 当前只展开为 `NORMAL/FIFO/DELAY/TRANSACTION`
- 并不会自动带上 `PRIORITY` 和 `LITE`

## gRPC 消息实体的类型转换

`GrpcConverter` 在把 `MessageExt` 转成 gRPC 消息时，会再次根据消息属性推导消息类型：

```java
TopicMessageType topicMessageType = TopicMessageType.parseFromMessageProperty(messageExt.getProperties());
systemPropertiesBuilder.setMessageType(convertToGrpcMessageType(topicMessageType));
```

但这里还有一个很容易忽略的细节：

```java
case UNSPECIFIED:
default:
    return MessageType.NORMAL;
```

也就是：

- 路由层的 `UNSPECIFIED` 会显式暴露成 `MESSAGE_TYPE_UNSPECIFIED`
- 消息实体转换层的 `UNSPECIFIED` 会回落成 `NORMAL`

这两个层面不能混为一谈。

## 这篇和 60 的区别

- [[04-Topic与消息类型约束]] 讲的是 Topic 属性和消息类型抽象本身。
- 本文讲的是这个抽象在 Proxy / gRPC 运行时如何真正生效。

## 还会影响哪些地方

### 1. [[04-Topic与消息类型约束]]SendMessageActivity / ProducerProcessor

- 发送前做 Topic 类型校验。

### 2. [[34-延迟消息设计]]RecallMessageActivity / ProducerProcessor

- 撤回前把期望类型固定为 `DELAY`。

### 3. [[35-事务消息设计]]RouteActivity

- 路由返回 `acceptMessageTypes`。

### 4. [[52-LiteMode与LiteTopic]]GrpcConverter

- 返回消息实体时再次推导消息类型。

## 研究时建议重点验证

- 关闭 `enableTopicMessageTypeCheck` 后，发送校验会不会放松，但路由暴露是否仍然保持原样。
- `MIXED` 在路由层和发送层是否完全对齐。
- `LITE` / `PRIORITY` 在 5.5.0 的路由暴露和实体转换覆盖度是否一致。

## 相关笔记

- [[04-Topic与消息类型约束]]

- [[35-事务消息设计]]