# OpenMessaging 标准接口

> 研究定位：看清 RocketMQ 5.5.0 里的 OpenMessaging 适配层到底有多“薄”，回答它是不是一套完整的新客户端实现。
> 关键源码：`openmessaging/src/main/java/io/openmessaging/rocketmq/MessagingAccessPointImpl.java`、`ProducerImpl.java`、`PushConsumerImpl.java`、`PullConsumerImpl.java`
> 阅读建议：这篇重点看适配层边界，不要预设 OpenMessaging 在 RocketMQ 里是核心主线。

## 先给结论

- OpenMessaging 适配层在 5.5.0 里是轻量包装层，不是 RocketMQ 的主接入模型。
- `MessagingAccessPointImpl` 主要负责根据属性创建 Producer / PushConsumer / PullConsumer 实例。
- 它没有提供完整的 StreamingConsumer 实现，`ResourceManager` 也明确标成不支持。
- 所以在 5.5.0 里，OpenMessaging 更像兼容层，而不是新能力承载层。

## `MessagingAccessPointImpl` 到底做了什么

源码非常直接：

- `createProducer()` -> `ProducerImpl`
- `createPushConsumer()` -> `PushConsumerImpl`
- `createPullConsumer()` -> `PullConsumerImpl`

这说明它本质上是：

- 一层对象工厂和属性拼装层

而不是复杂运行时内核。

## `attributes()` 和属性继承

`MessagingAccessPointImpl` 持有一份 `accessPointProperties`，后续创建实例时会把默认属性和调用方属性合并。

所以它的主要价值之一是：

- 在 OpenMessaging 抽象下统一属性入口

## 为什么说它是“薄适配”

几个最明显的证据：

### 1. [[06-生产者与消费者模型总览]]`implVersion()` 固定返回字符串

说明实现本身比较稳定、简单，不承担复杂协商。

### 2. `createStreamingConsumer()` 返回 `null`

这说明标准接口定义的能力并没有全部落地。

### 3. `resourceManager()` 直接抛 `OMSNotSupportedException`

说明 RocketMQ 只实现了接口的一个子集。

## OpenMessaging 和 RocketMQ 主流接口的关系

RocketMQ 自己的主流接入面包括：

- 经典 Java Client
- Proxy / gRPC

OpenMessaging 适配层并没有改变底层核心发送和消费能力，它只是：

- 把 RocketMQ 能力映射到 OMS 接口形态

所以如果研究 5.5.0 的主能力演进，优先级应该低于：

- classic client
- proxy / grpc

## 这篇最值得记住的点

- OpenMessaging 适配层是兼容层，不是主能力入口。
- `MessagingAccessPointImpl` 的职责非常轻。
- 标准接口中未完全支持的能力会显式返回空或抛异常。

## 和后续笔记的关系

- [[06-生产者与消费者模型总览]] 负责主流客户端模型。
-  一组笔记负责 5.x Proxy/gRPC 主接入面。

## 建议连读

1. [[06-生产者与消费者模型总览]]
2. [[07-客户端设计详解]]