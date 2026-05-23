# MessageRequestModeManager

> 研究定位：把 5.x 的 `MessageRequestMode` 从“一个枚举值”落到 Broker 本地持久化、assignment 默认值和 POP/PULL 分叉点上。
> 关键源码：`broker/.../MessageRequestModeManager.java`、`broker/.../QueryAssignmentProcessor.java`、`common/.../MessageRequestMode.java`
> 阅读建议：这篇一定和 [[53-5.x消费模型与负载均衡]] 一起读，否则很容易把它写成“只存个配置文件”的小点。

## 先给结论

- `MessageRequestMode` 是 Broker 侧的消息请求模式，只有 `PULL` 和 `POP` 两个值。
- 它不是经典 Java Client 的 API 类型，也不是 `LitePull` 这种客户端模型。
- `MessageRequestModeManager` 是 Broker 本地配置管理器，按 `topic -> consumerGroup -> requestBody` 存储。
- 这份配置的最重要消费者是 `QueryAssignmentProcessor`，也就是 5.x assignment 主线。
- retry topic 不允许自定义 mode，并且在 assignment 默认逻辑里会被强制成 `PULL`。

## 先把“客户端模型”和“请求模式”切开

下面这些不是同一层概念：

- `DefaultMQPushConsumer`
- `DefaultLitePullConsumer`
- `SIMPLE_CONSUMER`
- `MessageRequestMode.PULL`
- `MessageRequestMode.POP`

更准确地说：

- 前三者描述的是客户端/协议模型
- 后两者描述的是 Broker 认为这个 topic+group 应该如何请求消息

这也是 5.x 文档最容易讲乱的地方。

## 枚举本身非常简单，但语义不简单

定义只有：

```java
public enum MessageRequestMode {
    PULL("PULL"),
    POP("POP");
}
```

但真正重要的是它被放到了哪里：

- assignment 结果
- Broker 默认配置
- topic+group 粒度持久化
- 后续 ack/revive 语义

所以它不是“一个无关紧要的常量枚举”。

## `MessageRequestModeManager` 存的不是一个扁平字符串

核心表结构是：

```java
ConcurrentHashMap<String/*topic*/,
    ConcurrentHashMap<String/*consumerGroup*/, SetMessageRequestModeRequestBody>>
    messageRequestModeMap;
```

这说明它存的是：

- topic 维度
- group 维度
- 完整 request body

而不是简单的：

- `topic@group -> PULL/POP`

为什么要保存整个 `SetMessageRequestModeRequestBody`？

因为除了 `mode`，还要保存：

- `popShareQueueNum`

这会直接影响 POP assignment 共享队列语义。

## 它是 Broker 本地持久化配置

`MessageRequestModeManager` 继承 `ConfigManager`，配置文件路径走：

```java
BrokerPathConfigHelper.getMessageRequestModePath(storePathRootDir)
```

`QueryAssignmentProcessor` 构造时会：

```java
this.messageRequestModeManager = new MessageRequestModeManager(brokerController);
this.messageRequestModeManager.load();
```

而更新配置时会：

```java
this.messageRequestModeManager.setMessageRequestMode(topic, consumerGroup, requestBody);
this.messageRequestModeManager.persist();
```

所以它不是客户端内存态，也不是 NameServer 路由数据。

## `QueryAssignmentProcessor` 才是这份配置真正生效的地方

处理 `QUERY_ASSIGNMENT` 时，Broker 会先读：

```java
SetMessageRequestModeRequestBody body =
    this.messageRequestModeManager.getMessageRequestMode(topic, consumerGroup);
```

如果读不到，就走默认逻辑：

```java
if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
    body.setMode(MessageRequestMode.PULL);
} else {
    body.setMode(brokerController.getBrokerConfig().getDefaultMessageRequestMode());
}
if (body.getMode() == MessageRequestMode.POP) {
    body.setPopShareQueueNum(brokerController.getBrokerConfig().getDefaultPopShareQueueNum());
}
```

这里直接能落出三个结论：

- retry topic 默认强制 `PULL`
- 普通 topic 才回退到 Broker 默认 mode
- POP 默认值里还带 `defaultPopShareQueueNum`

## `setMessageRequestMode(...)` 有明确边界，不是想改就能改

Broker 处理 `SET_MESSAGE_REQUEST_MODE` 时会先做校验：

- retry topic 不允许设置
- topic 必须存在
- subscription group 必须存在

对应代码里最关键的一句就是：

```java
if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
    response.setCode(ResponseCode.NO_PERMISSION);
    response.setRemark("retry topic is not allowed to set mode");
}
```

这说明 message request mode 不是一个完全自由的运行时开关，它是有资源边界的。

## POP assignment 为什么要依赖它

`QueryAssignmentProcessor.doLoadBalance(...)` 在 clustering 模式下，会根据 mode 决定分配逻辑：

- `PULL`：走普通 `allocate(...)`
- `POP`：走 `allocate4Pop(...)`

而 `allocate4Pop(...)` 会使用：

- `popShareQueueNum`

来决定“一个 consumer 是否要共享后续若干 consumer 的队列”。

所以 `MessageRequestModeManager` 不是只影响请求码名字，而是直接影响 assignment 结果集合。

## 它和消费模型之间的准确关系

可以把关系记成这样：

- 客户端模型：应用怎么用
- 请求模式：Broker 怎么配发
- 消费闭环：后续怎么 ack / renew / revive

其中 `MessageRequestModeManager` 主要管第二层，并牵引第三层。

例如：

- `PULL` 更接近传统 offset 驱动
- `POP` 会牵出 invisible time、ack、changeInvisibleTime、revive

## 研究时最容易说错的点

- “LitePull 就是 PULL mode。”  
  不准确。LitePull 是客户端模型，不等于 Broker mode。

- “SimpleConsumer 就是 POP mode。”  
  也不能直接这么等价。虽然常相关，但两者分属协议模型和 Broker 请求模式。

- “MessageRequestMode 只影响查询 assignment 的一个字段。”  
  不准确。它会改变分配算法和后续消费语义。

## 建议连读

- [[53-5.x消费模型与负载均衡]]
- [[48-Pop消费模式详解]]