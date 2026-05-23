# LitePullConsumer 轻量级拉消费

> 研究定位：把 `DefaultLitePullConsumer` 从“更易用的 Pull API”拆到运行时骨架、offset 语义与 Broker 协议上。
> 关键源码：`client/.../DefaultLitePullConsumer.java`、`client/.../DefaultLitePullConsumerImpl.java`、`client/.../RebalanceLitePullImpl.java`、`client/.../MQClientAPIImpl.java`
> 阅读建议：先区分它和 [[54-SimpleConsumer与ReceiptHandle语义]] 完全不是一个概念，再和 [[46-Push消费与Pull消费]]、[[09-客户端Rebalance实现]] 对照。

## 先给结论

- `LitePullConsumer` 属于经典 Java Client，不属于 5.x gRPC `LITE_PUSH_CONSUMER`。
- 它不是“直接每次 `poll()` 去 Broker 拉一次”；`poll()` 只是在本地 `consumeRequestCache` 里取消息。
- 真正的拉取发生在每个 `MessageQueue` 对应的 `PullTaskImpl` 后台任务里。
- `subscribe` 和 `assign` 在实现上互斥，源码会直接抛出 `Subscribe and assign are mutually exclusive.`。
- clustering 模式下默认用 `RemoteBrokerOffsetStore`，broadcasting 模式下默认用 `LocalFileOffsetStore`。

## 它到底是什么

从类型上看：

- 对外入口是 `DefaultLitePullConsumer`
- 内部实现是 `DefaultLitePullConsumerImpl`
- Rebalance 骨架是 `RebalanceLitePullImpl`

这说明它仍然站在经典 Java Client 体系内，继续复用：

- `MQClientInstance`
- `PullAPIWrapper`
- `OffsetStore`
- `RebalanceImpl`

所以它不是一套“新协议客户端”，而是对传统 Pull 模型做了更强的封装和轮询式消费 API。

## 最容易搞错的三件事

### 1. 它和 LiteTopic 没关系

- `LitePullConsumer` 是客户端 API
- `LiteTopic` 是 5.5.0 新增的 Topic / Broker / Proxy 语义

两者名字相似，但实现模块、运行时路径、约束对象都不同。这个边界在  里已经单独拆开。

### 2. `poll()` 不是直接向 Broker 发请求

`poll(long timeout)` 的核心逻辑是：

```java
ConsumeRequest consumeRequest = consumeRequestCache.poll(...);
...
List<MessageExt> messages = consumeRequest.getMessageExts();
long offset = consumeRequest.getProcessQueue().removeMessage(messages);
assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
```

也就是：

- 后台拉取线程先把消息放进 `ProcessQueue`
- 再包装成 `ConsumeRequest` 放进 `consumeRequestCache`
- `poll()` 只是从本地缓存拿一批可消费消息

所以它更像“本地拉取缓冲 + 应用线程轮询消费”，不是同步 RPC 风格的直接 pull。

### 3. 默认并不是“必须手动提交”

`DefaultLitePullConsumer` 里：

```java
private boolean autoCommit = true;
private long autoCommitIntervalMillis = 5 * 1000;
```

`poll()` 开头会执行：

```java
if (defaultLitePullConsumer.isAutoCommit()) {
    maybeAutoCommit();
}
```

所以更准确的说法是：

- 默认开启自动提交
- 如果你关闭 `autoCommit`，才需要明确调用 `commitSync()`

旧式教程里常把它统一说成“手动提交模型”，这在 5.5.0 源码语义上并不精确。

## 运行时骨架

### 启动阶段

`DefaultLitePullConsumerImpl.start()` 的主线是：

1. `checkConfig()`
2. `initScheduledThreadPoolExecutor()`
3. `initMQClientFactory()`
4. `initRebalanceImpl()`
5. `initPullAPIWrapper()`
6. `initOffsetStore()`
7. `mQClientFactory.start()`
8. `startScheduleTask()`
9. `operateAfterRunning()`

其中几个关键点：

- clustering 模式会 `changeInstanceNameToPID()`
- 启动后会 `checkClientInBroker()`
- 还会按 `topicMetadataCheckIntervalMillis` 周期检查 topic metadata 变化

### offset store 默认选择

`initOffsetStore()` 里分支很明确：

```java
case BROADCASTING:
    this.offsetStore = new LocalFileOffsetStore(...);
case CLUSTERING:
    this.offsetStore = new RemoteBrokerOffsetStore(...);
```

所以：

- 广播模式默认本地文件存 offset
- 集群模式默认 Broker 远端存 offset

这点和 PushConsumer 的经典语义一致。

## subscribe 与 assign 的边界

实现里用一个内部枚举 `SubscriptionType { NONE, SUBSCRIBE, ASSIGN }` 管状态，并通过：

```java
private static final String SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE =
    "Subscribe and assign are mutually exclusive.";
```

来保证两者互斥。

### subscribe 模式

特点：

- 把订阅信息放进 `rebalanceImpl.getSubscriptionInner()`
- 由 `RebalanceLitePullImpl` 参与队列分配
- `MessageQueueListener` 变化时更新 assigned queue 并启动/停掉对应 `PullTask`

本质上它仍然是“有 Rebalance 的 LitePull”。

### assign 模式

特点：

- 直接 `assignedMessageQueue.updateAssignedMessageQueue(messageQueues)`
- 不依赖 Rebalance 分配结果
- 允许在启动前通过 `setSubExpressionForAssign(...)` 绑定筛选表达式

这是一种“手工指定队列 + 后台持续拉取 + 应用线程轮询取消息”的模式。

## 真正的拉取在哪里发生

核心在 `PullTaskImpl.run()`。

### 每个队列一个拉取任务

`startPullTask(Collection<MessageQueue> mqSet)` 会给每个 queue 创建一个 `PullTaskImpl` 并交给 `scheduledThreadPoolExecutor`。

这意味着：

- LitePull 不是单线程串行拉全局
- 它是按 queue 维度并发拉取

### 拉取前有多级流控

`PullTaskImpl` 里会先检查：

- `consumeRequestCache.size() * pullBatchSize > pullThresholdForAll`
- `cachedMessageCount > pullThresholdForQueue`
- `cachedMessageSizeInMiB > pullThresholdSizeForQueue`
- `processQueue.getMaxSpan() > consumeMaxSpan`

命中后不会继续拉，而是延后调度自己。

这说明 LitePull 的“轻量”不是无缓冲，而是有明确的本地缓存和流控边界。

### 拉取 offset 的确定

下一次拉取 offset 由 `nextPullOffset(messageQueue)` 决定，优先级是：

1. `seekOffset`
2. 已记录的 pull offset
3. `fetchConsumeOffset()` 计算出的初始消费位点

也就是：

- `seek()` 会重写下一次拉取起点
- 没有 `seek()` 时才走正常 offset 演进

## `poll()`、`seek()`、`commitSync()` 的真实语义

### `poll()`

`poll()` 不直接拉 Broker，它只做三件事：

1. 可选自动提交
2. 从 `consumeRequestCache` 取一条 `ConsumeRequest`
3. 从对应 `ProcessQueue` 删除已返回消息并更新 consume offset

所以 `poll()` 的阻塞，主要是在等本地缓存有可消费数据，而不是直接等远端响应。

### `seek()`

`seek()` 会：

1. 校验 queue 当前确实在 assigned 集合里
2. 校验 offset 在 `[minOffset, maxOffset]`
3. 清理本地缓存 `clearMessageQueueInCache(messageQueue)`
4. 中断旧 `PullTaskImpl`
5. 写入新的 `seekOffset`
6. 重新启动该 queue 的拉取任务

因此 `seek()` 不只是改一个数字，它会主动重建该队列的本地拉取状态。

### `commitSync()`

`DefaultLitePullConsumer.commitSync()` 最终调用 `defaultLitePullConsumerImpl.commitAll()`。

实现上它提交的是 `assignedMessageQueue` 当前维护的 consume offset，而不是“你手里这批消息的某个 ack”。

这和 5.x `SIMPLE_CONSUMER` 的 `ReceiptHandle` / ack 语义完全不是一回事，后者见 [[54-SimpleConsumer与ReceiptHandle语义]]。

## 它发给 Broker 的请求是什么

`pullSyncImpl(...)` 会构造：

```java
int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false, true);
```

然后进入 `pullAPIWrapper.pullKernelImpl(...)`。

在 `MQClientAPIImpl.pullMessage(...)` 里，如果 `sysFlag` 带了 lite 标记，就会发：

```java
RequestCode.LITE_PULL_MESSAGE
```

否则才是普通 `PULL_MESSAGE`。

Broker 侧 `PullMessageProcessor` 还会检查：

```java
if (request.getCode() == RequestCode.LITE_PULL_MESSAGE
    && !brokerConfig.isLitePullMessageEnable()) {
    ...
}
```

所以 LitePull 并不只是客户端 API 名字不同，它在 remoting 请求码上也单独分流。

## 和 PushConsumer 的真实差异

### 相同点

- 都复用 `MQClientInstance`
- 都复用 `RebalanceImpl`
- 底层都还是 Pull 协议
- clustering / broadcasting 的 offset 语义一致

### 不同点

- Push 把拉取、回调消费、重试都包进 SDK
- LitePull 把最终消费节奏交还给应用线程的 `poll()`
- Push 用 `ConsumeMessageService`
- LitePull 用 `consumeRequestCache + poll()`

所以研究时不要把它看成“旧 PullConsumer 的一个别名”，它更接近“带本地预取与队列任务调度的轮询消费器”。

## 研究时要盯住的问题

- `subscribe` 模式下它和 [[09-客户端Rebalance实现]] 的重合度到底有多高。
- `assign` 模式绕开了哪些 Rebalance 能力，又保留了哪些共享运行时组件。
- `LITE_PULL_MESSAGE` 和普通 `PULL_MESSAGE` 在 Broker 侧到底多了哪些约束。
- 自动提交开启时，应用层对“已处理”和“已提交”的时序是否容易误判。

## 相关笔记

- [[09-客户端Rebalance实现]]
- [[51-MessageRequestModeManager]]

- [[54-SimpleConsumer与ReceiptHandle语义]]