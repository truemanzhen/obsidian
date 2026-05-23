# 客户端 Producer 内部实现

> 研究定位：把 Producer 从“send 一次 RPC”拆成路由缓存、队列选择、故障回避、发送主链、hook 注入和 retry topic 特殊处理。
> 关键源码：`client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java`、`client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java`、`client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java`
> 阅读建议：先看 [[06-生产者与消费者模型总览]]，再回来读发送路径细节。

## 先给结论

- Producer 的真实骨架不是 `DefaultMQProducer`，而是 `DefaultMQProducerImpl + TopicPublishInfo + MQFaultStrategy + MQClientInstance`。
- 发送主线不是“查路由后发包”这么简单，中间还夹着队列选择、超时截断、故障回避和 hook 处理。
- 同步发送才有显式多次重试循环；异步和单向发送不是旧教程里那种完全等价的重试模型。
- 队列选择不只是轮询，`MQFaultStrategy` 会把延迟和 reachability 转成 broker 的回避策略。
- `sendKernelImpl(...)` 还会处理 unique ID、压缩、事务消息标记、namespace、retry topic 附加属性等运行时语义。

## Producer 主线到底分哪几层

### API 层

- `DefaultMQProducer`

### 实现层

- `DefaultMQProducerImpl`

### 路由层

- `TopicPublishInfo`

### 容灾层

- `MQFaultStrategy`

### 共享客户端底座

- `MQClientInstance`
- `MQClientAPIImpl`

研究 Producer 时，最稳的切法不是“记住几个 send 重载”，而是沿这几层往下钻。

## `sendDefaultImpl(...)` 是发送主链骨架

这段代码的主顺序非常清晰：

```java
Validators.checkMessage(msg, this.defaultMQProducer);
TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
...
MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName, resetIndex);
sendResult = this.sendKernelImpl(...);
this.updateFaultItem(...);
```

翻成运行时步骤就是：

1. [[06-生产者与消费者模型总览]]校验消息
2. [[07-客户端设计详解]]找 topic 路由
3. [[10-Offset管理机制]]选 queue
4. 发包
5. 根据发送结果更新 broker 容灾状态

所以 Producer 的关键不是“会不会发”，而是“发之前和发之后各做了哪些决策”。

## 路由查找是缓存优先，不是每次都查 NameServer

`tryToFindTopicPublishInfo(...)` 会先查本地 `topicPublishInfoTable`。

只有缓存缺失或不可用时，才会：

```java
this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
```

这说明 Producer 路由模型本质上是：

- 本地缓存
- 缺失时刷新
- 再使用刷新后的发布视图

这和  里的共享客户端运行时缓存是同一条主线。

## 队列选择不是简单轮询

`TopicPublishInfo` 自己确实提供了基础轮询：

```java
int index = this.sendWhichQueue.incrementAndGet();
int pos = index % this.messageQueueList.size();
return this.messageQueueList.get(pos);
```

但 `DefaultMQProducerImpl` 并不直接止步于此，而是调用：

```java
this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName, resetIndex);
```

所以真实选择逻辑是：

- `TopicPublishInfo` 提供候选与基础轮询
- `MQFaultStrategy` 决定是否过滤故障 broker

## `MQFaultStrategy` 负责把“发送慢”变成“暂时别再选这个 broker”

它会维护：

- `availableFilter`
- `reachableFilter`
- `BrokerFilter(lastBrokerName)`

开启延迟故障感知后，选择顺序是：

1. [[06-生产者与消费者模型总览]]优先选 available broker
2. [[07-客户端设计详解]]不行再选 reachable broker
3. [[10-Offset管理机制]]再不行退回普通轮询

对应代码核心是：

```java
mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter);
...
mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter);
...
return tpInfo.selectOneMessageQueue();
```

所以 Producer 的“负载均衡”本质上是“轮询 + 故障回避”的组合。

## 故障回避不是二元开关，而是延迟映射

`updateFaultItem(...)` 会把当前延迟映射成一个不可用时长：

```java
long duration = computeNotAvailableDuration(isolation ? 10000 : currentLatency);
this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
```

这意味着：

- broker 并不是只有“活 / 死”两种状态
- 发送变慢也会让客户端短时间减少选择它

这点是旧教程里经常被简化掉的关键实现。

## 同步、异步、单向发送不能混着记

`sendDefaultImpl(...)` 里有一句很关键：

```java
int timesTotal = communicationMode == CommunicationMode.SYNC
    ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed()
    : 1;
```

这说明：

- 同步发送才会进入显式多次尝试循环
- 异步 / 单向不会复用同样的同步重试模型

所以“Producer 默认都会重试多次”这个说法本身就不严谨。

## `sendKernelImpl(...)` 才是真正发包前的运行时加工点

它在发出请求前会处理：

- broker 地址查找
- VIP channel 选择
- unique ID 注入
- 消息体压缩
- 事务消息 prepared 标记
- send hook / forbidden hook
- 构造 `SendMessageRequestHeader`

这意味着 Producer 不是“拿到 Message 就原样透传给 Broker”，而是会在客户端侧做大量运行时整形。

## retry topic 消息还有一条特殊处理支线

如果 topic 以 `%RETRY%` 前缀开头，`sendKernelImpl(...)` 会额外从消息属性里提取：

- `PROPERTY_RECONSUME_TIME`
- `PROPERTY_MAX_RECONSUME_TIMES`

并回写到请求头里。

这说明 Producer 发送 retry topic 消息时，并不是和普通消息完全同构。

它会携带额外的消费重试语义。

## namespace 和重发是耦合的

发送中如果开启 namespace，Producer 会：

- 设置 `instanceId`
- 必要时在重发场景下恢复 topic 的 namespace 视图

所以 namespace 不是单纯的“字符串前缀拼接”，而是进入发送路径的运行时语义。

## `retryAnotherBrokerWhenNotStoreOK` 的含义要讲准

同步发送时，如果 `sendResult.getSendStatus() != SEND_OK`，且配置允许，就可能继续换 broker 重试。

这说明：

- 是否重试不只看抛异常
- 某些“响应成功但状态不理想”的结果也会触发继续尝试

这也是为什么 Producer 可能带来重复消息，业务侧仍要做幂等。

## 这篇要纠正的几个旧说法

- “Producer 就是先轮询选 queue 再发一次 RPC。”  
  不对，还有路由缓存、故障回避和发送后 fault 更新。

- “同步 / 异步 / 单向只是回调形式不同。”  
  不对，重试模型就已经不同。

- “发送失败只是简单换个 broker 再试。”  
  不对，是否重试、如何回避 broker，都受 `MQFaultStrategy` 和响应状态影响。

- “消息是原样发给 Broker 的。”  
  不对，客户端还会做 unique ID、压缩、事务标记和 hook 注入。

## 建议连读顺序

1. [[06-生产者与消费者模型总览]]
2. [[07-客户端设计详解]][[06-生产者与消费者模型总览]]
3. [[10-Offset管理机制]]

## 相关笔记

- [[06-生产者与消费者模型总览]]