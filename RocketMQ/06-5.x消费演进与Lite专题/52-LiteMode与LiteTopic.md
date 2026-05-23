# LiteMode 与 LiteTopic

> 研究定位：把 RocketMQ 5.5.0 里所有带 “Lite” 的概念彻底拆开。
> 关键源码：`TopicMessageType.LITE`、`LiteUtil`、`LiteManagerProcessor`、`LiteSubscriptionCtlProcessor`、`LiteSubscriptionRegistry`
> 阅读建议：先区分 `LitePullConsumer` 和 `LiteTopic`，再去看 `LITE_PUSH_CONSUMER` 这类协议侧概念。

## 先给结论

- `LitePullConsumer` 和 `LiteTopic` 不是一回事。
- 前者是经典 Java Client 里的一个消费 API。
- 后者是 5.5.0 新增的一套 Topic / Broker / Proxy 级语义，底层会落到 LMQ 命名和 Lite 订阅生命周期管理上。

## 这篇为什么必须单列

如果只看名字，最容易误判成：

- “LitePullConsumer 就是消费 LiteTopic 的客户端”

但从源码看，实际不是这样：

- `DefaultLitePullConsumer` 在 `client/` 模块
- `LiteTopic` / `LITE_PUSH_CONSUMER` / `LiteSubscriptionRegistry` 在 `broker/`、`proxy/`、`common/` 模块

它们属于不同层面的 “Lite”。

## LiteTopic 的三个核心对象

### 1. Topic 类型

- `TopicMessageType.LITE`
- `TopicConfig.setLiteTopicExpiration(...)`

这说明 Lite 是 Topic 级别的消息语义，不是某个 API 的别名。

### 2. 逻辑命名

`LiteUtil.toLmqName(parentTopic, liteTopic)` 会把资源转成 LMQ 风格的逻辑队列名，例如：

```text
%LMQ%$parentTopic$liteTopic
```

这意味着 LiteTopic 在 Broker 侧并不是“另起一套队列结构”，而是映射到 LMQ 语义。

源码注释还明确说明了一件很重要的事：

- Lite Topic 是“implemented based on LMQ”
- 但 “lmq is not necessarily a lite topic”
- Lite Topic “has no retry topic”

也就是：

- `LMQ` 是更底层的队列实现能力
- `LiteTopic` 是建立在 `LMQ` 之上的一层有约束的主题语义

### 3. 订阅生命周期

核心组件包括：

- `LiteSubscriptionCtlProcessor`
- `LiteSubscriptionRegistry`
- `AbstractLiteLifecycleManager`
- `LiteEventDispatcher`
- `LiteSharding`

也就是说，LiteTopic 不只是命名规则，它还有自己的：

- 订阅注册
- TTL 清理
- 分片归属
- 事件分发

## Broker / Proxy 是怎么识别 Lite 的

### 1. 消息属性与 Topic 类型

- 消息属性里会出现 `PROPERTY_LITE_TOPIC`
- `TopicMessageType.parseFromMessageProperty(...)` 会把它推导成 `LITE`

### 2. gRPC 客户端类型

Proxy / gRPC 侧还会出现：

- `ClientType.LITE_PUSH_CONSUMER`

`ReceiveMessageActivity` 会据此区分 Lite 消费路径，这和 `DefaultLitePullConsumer` 又不是一个维度。

更具体一点，`ClientActivity` 里会把它映射成：

- `ConsumeType.CONSUME_PASSIVELY`
- `MessageModel.LITE_SELECTIVE`

所以：

- `LITE_PUSH_CONSUMER` 是协议侧客户端类型
- `DefaultLitePullConsumer` 是经典 Java Client API
- 二者都带 “Lite”，但不属于同一抽象层

### 3. SubscriptionGroup 绑定

`SubscriptionGroupConfig` 里有：

- `liteBindTopic`
- `liteSubExclusive`
- `liteSubClientQuota`

这说明 Lite 消费模型还会反向约束消费组配置。

## Lite 运行时约束到底落在哪

### 1. Lite 管理入口先校验 TopicMessageType

`LiteManagerProcessor` 和 `PopLiteMessageProcessor` 都会先检查：

```java
if (!TopicMessageType.LITE.equals(topicConfig.getTopicMessageType())) {
    response.setCode(ResponseCode.INVALID_PARAMETER);
    ...
}
```

所以 Lite 不是“消息里打个 `PROPERTY_LITE_TOPIC` 就能跑”，而是 parent topic 自身必须是 `LITE`。

### 2. group 还要和 parent topic 绑定

`LiteManagerProcessor` / `PopLiteMessageProcessor` 还会检查：

```java
if (!parentTopic.equals(groupConfig.getLiteBindTopic())) {
    response.setCode(ResponseCode.INVALID_PARAMETER);
    ...
}
```

也就是：

- group 不是任意挂到 LiteTopic 上
- 它必须先通过 `liteBindTopic` 绑定到 parent topic

### 3. ack / 续期都要重新回到 LMQ 名字

Broker 处理 ack / changeInvisibleTime 时不会直接用 parent topic，而是：

```java
String lmqName = LiteUtil.toLmqName(requestHeader.getTopic(), requestHeader.getLiteTopic());
```

这说明 LiteTopic 在消费运行时真正落地的对象仍然是：

- `parentTopic + liteTopic -> lmqName`

而不是额外独立出一套物理 Topic。

## 建议和哪些笔记一起看

- [[50-LitePullConsumer轻量级拉消费]]：客户端 API 视角的 LitePull
- [[51-MessageRequestModeManager]]：Broker 侧消费请求模式
- [[53-5.x消费模型与负载均衡]]：5.x 消费抽象如何纳入 Lite
- [[45-TopicMessageType校验与路由行为]]：`LITE` 类型怎样参与校验和路由

## 这篇要重点验证的问题

- LiteTopic 和普通 Topic 的最小差异集合是什么。
- `LITE_PUSH_CONSUMER` 与 `DefaultLitePullConsumer` 的语义边界是什么。
- LMQ 命名只是存储映射，还是还会影响路由 / 负载均衡 / ACL。
- LiteTopic 的 TTL 清理对 offset / lag / 订阅恢复有哪些副作用。