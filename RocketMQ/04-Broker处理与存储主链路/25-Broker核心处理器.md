# Broker 核心处理器

> 研究定位：把 Broker 入口处理层从“RequestCode 对应哪个 Processor”推进到“哪些请求真正决定发送、拉取、POP、offset、assignment 和管理行为”。
> 关键源码：`broker/src/main/java/org/apache/rocketmq/broker/processor/`、`broker/src/main/java/org/apache/rocketmq/broker/longpolling/`、`broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java`
> 阅读建议：先把 `BrokerController` 的处理器注册表和具体 Processor 的职责边界一起看，不要把所有 Processor 当成平级知识点死记。

## 先给结论

- Broker 处理器层本质上是 remoting 请求到存储/消费语义的第一层翻译器。
- 真正值得优先研究的不是“有哪些类”，而是 5 条主链：发送、传统拉取、POP、offset 管理、assignment 管理。
- `SendMessageProcessor` 和 `AbstractSendMessageProcessor` 不只负责生产者发送，也承接经典 `consumerSendMsgBack(...)`。
- `PullMessageProcessor`、`PopMessageProcessor`、`AckMessageProcessor`、`ChangeInvisibleTimeProcessor` 共同组成了 RocketMQ 5.5.0 两套消费语义。
- `QueryAssignmentProcessor` 在 5.5.0 很关键，因为它把 Broker 默认请求模式、POP assignment 和服务端负载均衡显式拉进消费主线。

## 先看处理器层在 Broker 里的位置

从架构上看，Broker 处理器层介于：

- remoting 网络入口
- 存储层 / 消费管理 / 元数据管理

之间。

更准确地说，它负责把：

- 请求头
- topic / group / queue 上下文
- 权限与元数据校验

转换成对：

- `MessageStore`
- `TopicConfigManager`
- `SubscriptionGroupManager`
- `ConsumerOffsetManager`
- 长轮询服务

的具体调用。

## 研究顺序不要按类名平均铺开

更稳的顺序是：

1. [[22-消息处理链路总览]]`SendMessageProcessor`
2. [[23-SendMessageActivity]]`PullMessageProcessor`
3. `PopMessageProcessor`
4. [[24-ReceiveMessageActivity]]`ConsumerManageProcessor`
5. [[31-刷盘机制]]`QueryAssignmentProcessor`
6. [[33-消息存储清理与回溯边界]]再回头看 `AdminBrokerProcessor`

因为这几条链直接构成了大部分业务运行时行为。

## `SendMessageProcessor` 有两层职责

### 1. [[22-消息处理链路总览]]正常发送

也就是生产者消息写入。

### 2. [[23-SendMessageActivity]]经典消费失败回投

`SendMessageProcessor` 会分流到：

```java
return this.consumerSendMsgBack(ctx, request);
```

而 `consumerSendMsgBack(...)` 实现在 `AbstractSendMessageProcessor`。

这意味着：

- 经典消费失败重试不是某个单独“重试处理器”负责
- 它就在发送处理骨架里

这点对理解  非常关键。

## `AbstractSendMessageProcessor` 才是发送主链公共骨架

它统一提供了：

- topic / body / properties 校验
- 构造 `MessageExtBrokerInner`
- send hook / consume hook
- `consumerSendMsgBack(...)`

### send-back 关键分支

`consumerSendMsgBack(...)` 里最重要的几句是：

```java
String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
...
int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
...
if (msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0) {
    newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
    queueIdInt = randomQueueId(DLQ_NUMS_PER_GROUP);
}
```

这里直接给出几个结论：

- 经典重试默认先投 `%RETRY%group`
- 是否进 DLQ 由 broker 决定
- `DLQ_NUMS_PER_GROUP = 1`
- `retryMaxTimes` 的来源是 group 配置而不是客户端硬编码总规则

## 发送处理器不只懂“普通消息”

从发送主线看，处理器层至少还要理解：

- 延迟消息
- 事务消息
- 顺序/FIFO 语义
- recall / reply / 静态 topic 等扩展能力

所以“Broker 核心处理器”这篇不应该停在类名罗列，而要记住：

- 发送主链是消息语义收口点

## `PullMessageProcessor` 仍是传统消费主线入口

传统 pull / push 消费最终都会落到 `PULL_MESSAGE` 语义。

`PullMessageProcessor` 的核心职责是：

- 校验 topic / group / 权限
- 解析订阅表达式
- 构建 message filter
- 调 `messageStore.getMessage(...)`
- 在无新消息时接入长轮询

所以 Push 模型虽然用户侧像回调消费，但 Broker 入口仍然是这条 pull 处理链。

## 长轮询不是独立功能，而是拉取处理器的后半段

`PullMessageProcessor` 在没有新消息但允许长轮询时，会把请求挂给：

- `PullRequestHoldService`

这说明：

- “有没有实时性”
- “会不会空轮询”

不是另一个系统决定的，而是拉取处理器和长轮询服务联动的结果。

详细见 [[47-长轮询机制详解]]。

## `PopMessageProcessor` 不是传统 Pull 的小改版

POP 处理器本质上引入了另一套 Broker 消费语义：

- 返回消息时附带 invisible window
- 需要后续 ack
- 未 ack 时要进 revive 闭环

所以它不像 `PullMessageProcessor` 那样只返回“消息 + 下一个 offset”，而是还要维护：

- check point
- retry topic 选择
- 可见性时间窗口

因此：

- POP 不能只被理解成“Pull 的另外一个 RequestCode”
- 它牵动的是后续 `AckMessageProcessor` 与 `ChangeInvisibleTimeProcessor`

## `AckMessageProcessor` 和 `ChangeInvisibleTimeProcessor` 是 POP 闭环的一部分

如果只看 `PopMessageProcessor`，你看到的只是“取走消息”。

真正闭环还包括：

- `AckMessageProcessor`
- `ChangeInvisibleTimeProcessor`
- `PopReviveService`

这条链回答的是：

- 消费成功如何确认
- 超时时间如何续期
- 未确认消息如何重投

所以 Broker 处理器层在 5.5.0 已经不再是“发送一个处理器、消费一个处理器”那么简单。

## `ConsumerManageProcessor` 是 offset 协议面的实际入口

它直接处理：

- `QUERY_CONSUMER_OFFSET`
- `UPDATE_CONSUMER_OFFSET`
- `GET_CONSUMER_LIST_BY_GROUP`

这里最重要的不是命令名，而是它把 Broker 侧 offset 管理和 Topic/Group 校验真正接到协议面上。

例如更新 offset 前会检查：

- group 是否存在
- topic 是否存在
- queueId / offset 合法性
- 是否命中 server-side reset 场景

所以 offset 不是“Broker 自己默默存起来”的黑盒行为，而是处理器层明确暴露的协议面。

## `QueryAssignmentProcessor` 是 5.x 消费主线的关键新增节点

这篇如果不讲它，就会把 5.5.0 讲成 4.x 口径。

它做的事情包括：

- 读取 `MessageRequestModeManager`
- 决定默认请求模式
- retry topic 强制 `PULL`
- `POP` 时带上 `defaultPopShareQueueNum`
- 在 `serverLoadBalancerEnable` 开关下决定是否由服务端参与 assignment
- `POP` 模式下用 `allocate4Pop(...)` 改写队列共享语义

这意味着 Broker 处理器层已经不只是“被动读写消息”。

它还会：

- 直接参与消费模式协商
- 直接参与负载均衡结果生成

## `QueryAssignmentProcessor` 里几个必须记住的源码事实

### 默认请求模式不是永远 `PULL`

```java
setMessageRequestModeRequestBody.setMode(
    brokerController.getBrokerConfig().getDefaultMessageRequestMode()
);
```

### 但 retry topic 会被强制拉回 `PULL`

```java
if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
    setMessageRequestModeRequestBody.setMode(MessageRequestMode.PULL);
}
```

### `POP` 时会注入共享队列参数

```java
setMessageRequestModeRequestBody.setPopShareQueueNum(
    brokerController.getBrokerConfig().getDefaultPopShareQueueNum()
);
```

### 关闭服务端负载均衡时，会直接返回全部订阅队列集合

```java
if (!brokerController.getBrokerConfig().isServerLoadBalancerEnable()) {
    return mqSet;
}
```

所以 5.5.0 的消费主线里，“Broker 是否参与 assignment”本身就是一个运行时配置边界。

## `AdminBrokerProcessor` 更像管理面多路复用器

它当然很大，也承接大量命令：

- Topic / Group 管理
- Broker 配置更新
- runtime info
- message query
- reset offset
- 各类治理命令

但研究优先级上，它更像“运维入口总表”，而不是最先该深挖的业务链主干。

对研究版文档来说，正确姿势是：

- 在这里明确它的治理入口地位
- 具体命令语义下沉到  和相关专题

## 处理器层和长轮询服务层是联动关系

处理器层并不独占全部消费逻辑。

实际运行时要连着看：

- `PullRequestHoldService`
- `PopLongPollingService`
- `PopLiteLongPollingService`
- `NotifyMessageArrivingListener`

因为：

- 请求挂起由这些服务接管
- 唤醒时机也不在具体 Processor 单点里

## 这篇要纠正的几个旧说法

- “Broker 核心处理器就是把请求分发给存储层。”  
  它还承担消费模式、offset 协议、assignment 协商和重试分流。

- “消费失败重试是单独的 retry 处理器负责。”  
  经典链路在 `AbstractSendMessageProcessor.consumerSendMsgBack(...)`。

- “POP 只是多了一个取消息接口。”  
  它还引入 ack、续期、revive 和共享队列语义。

- “负载均衡完全由客户端自己算。”  
  5.5.0 里 `QueryAssignmentProcessor` 已能在 Broker 侧参与 assignment。

## 建议连读顺序

1. [[22-消息处理链路总览]][[22-消息处理链路总览]]
2. [[23-SendMessageActivity]][[23-SendMessageActivity]]
3. [[47-长轮询机制详解]]
4. [[24-ReceiveMessageActivity]]
5. [[31-刷盘机制]]
6. [[33-消息存储清理与回溯边界]]

## 相关笔记

- [[22-消息处理链路总览]]

- [[47-长轮询机制详解]]