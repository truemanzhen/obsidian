# Push 消费与 Pull 消费

> 研究定位：把“Push / Pull / Pop”这组容易混淆的消费概念拆成经典客户端模型、Broker 请求模式与 5.x assignment 三层。
> 关键源码：`client/.../DefaultMQPushConsumer.java`、`client/.../DefaultMQPushConsumerImpl.java`、`broker/.../PullMessageProcessor.java`、`broker/.../PullRequestHoldService.java`、`client/.../RebalanceImpl.java`
> 阅读建议：本篇只建立经典模型底座；5.x 的 `POP` / `MessageRequestMode` / `QueryAssignment` 请接 [[51-MessageRequestModeManager]]、[[53-5.x消费模型与负载均衡]]、[[48-Pop消费模式详解]]。

## 先给结论

- `PushConsumer` 在源码注释里就明确写了：它本质是对底层 pull service 的包装，不是 Broker 真推送。
- `PullConsumer` / `LitePullConsumer` / `PushConsumer` 都仍然建立在 pull 协议上。
- `POP` 不是 `Push` 的别名，也不是“Broker 主动把消息推给客户端”。
- 到 5.5.0，应该把“客户端 API 模型”和“Broker 请求模式”分开看。

## 概念拆分

### 经典客户端 API 模型

- `DefaultMQPushConsumer`
- `DefaultMQPullConsumer`
- `DefaultLitePullConsumer`

这是“你在 Java 里怎么写代码”的视角。

### Broker 请求模式

- `PULL`
- `POP`

这是“服务端按什么语义给你返回消息”的视角，见 [[51-MessageRequestModeManager]]。

所以：

- `Push` 不是 broker 请求模式
- `LitePull` 也不是 broker 请求模式

## Pull 模型的底座

### Broker 侧入口

经典拉取走 `PullMessageProcessor`，请求码可能是：

- `PULL_MESSAGE`
- `LITE_PULL_MESSAGE`

其中 `LITE_PULL_MESSAGE` 只是 LitePull 的 remoting 分流，不代表它跳出了 pull 模型。

### 处理主线

`PullMessageProcessor.processRequest(...)` 主要做：

1. [[06-生产者与消费者模型总览]]解析 `PullMessageRequestHeader`
2. [[07-客户端设计详解]]权限和订阅组校验
3. 构造 `MessageFilter`
4. 调用 `messageStore.getMessage(...)`
5. [[50-LitePullConsumer轻量级拉消费]]根据 `GetMessageStatus` 决定立即返回还是进入长轮询

所以 Pull 的本质就是：

- 客户端明确给 topic / queue / offset
- Broker 根据当前存储状态返回消息或挂起请求

### 长轮询不是 Push，仍然是 Pull

没有新消息时，请求会进入 `PullRequestHoldService.suspendPullRequest(...)`。

后续由：

- 定时检查
- 或消息到达时 `notifyMessageArriving(...)`

来唤醒请求。

这就是 RocketMQ 常说的“长轮询模拟推送”。

## PushConsumer 为什么不是真推送

`DefaultMQPushConsumer` 类注释原话就说明它是 underlying pull service 的 wrapper。

换成运行时主线，可以理解为：

1. [[06-生产者与消费者模型总览]]`RebalanceImpl` 决定当前 client 持有哪些 queue
2. [[07-客户端设计详解]]`PullMessageService` 调度每个 `PullRequest`
3. `DefaultMQPushConsumerImpl.pullMessage(...)` 持续向 Broker 发 pull
4. 拉到消息后交给 `ConsumeMessageService`
5. [[50-LitePullConsumer轻量级拉消费]]再回调业务 listener

所以 Push 的真实形态是：

- queue 仍然归属给 client
- client 仍然持续发 pull
- SDK 只是把“拉取 + 回调 + 提交 + 重试”封装掉了

## Push 和 Pull 的真正差异

### Pull

- 应用自己管理拉取节奏
- 应用自己决定何时处理下一批
- offset / 线程模型 / 缓冲策略更多暴露给调用方

### Push

- SDK 内部持续拉取
- SDK 把消息投递给 listener
- Rebalance、消费线程池、重试策略都内建

所以 Push 与 Pull 的区别主要在客户端封装层，而不是 Broker 读消息的根本协议被替换了。

## 5.5.0 为什么还要把本篇单独留着

因为如果不先把经典 Pull/Push 看清，后面很容易把这些概念混在一起：

- 以为 `POP` 就是新的 Push
- 以为 LitePull 是 LiteTopic 客户端
- 以为 5.x 消费模型已经完全不走 Rebalance

这些都不准确。

## 和 POP 的边界

本篇只给边界，不展开细节。

### POP 改变了什么

- assignment 结果里会带 `MessageRequestMode.POP`
- `RebalanceImpl.updateMessageQueueAssignment(...)` 会把 assignment 分成 push/pull 队列和 pop 队列两套表
- 对 POP 队列会创建 `PopRequest`，再通过 `dispatchPopPullRequest(...)` 进入 `DefaultMQPushConsumerImpl.popMessage(...)`

这说明在经典 Java Client 路径里：

- POP 不是凭空绕过客户端全部调度
- 它仍然经过 assignment / rebalance 结果更新
- 真正变化的是“按 queue pull 并提交 offset”变成“按 pop 语义取消息并走 ack / invisibleTime / revive”

因此“POP 完全不需要 Rebalance”这个说法太粗糙，最多只能说它改变了传统 queue ownership 的语义。

## 研究时应该怎样记忆

先按两条轴记：

### 轴一：客户端 API

- Pull
- Push
- LitePull

### 轴二：Broker 请求模式

- PULL
- POP

再把 5.x 的服务端 assignment 作为第三层叠上去。

这样后面看 [[53-5.x消费模型与负载均衡]] 时才不会把层次搞乱。

## 建议阅读顺序

1. [[06-生产者与消费者模型总览]]
2. [[07-客户端设计详解]]
3. [[50-LitePullConsumer轻量级拉消费]]
4. [[51-MessageRequestModeManager]]
5. [[50-LitePullConsumer轻量级拉消费]]
6. [[53-5.x消费模型与负载均衡]]