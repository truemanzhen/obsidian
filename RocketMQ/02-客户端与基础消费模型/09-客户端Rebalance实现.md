# 客户端 Rebalance 实现

> 研究定位：把 `RebalanceImpl` 从“均衡分配队列”的口号，拆到本地 rebalance、broker assignment、Push/Pop/LitePull 三条执行分支。
> 关键源码：`client/.../RebalanceImpl.java`、`client/.../RebalancePushImpl.java`、`client/.../RebalanceLitePullImpl.java`、`client/.../RebalanceService.java`、`broker/.../QueryAssignmentProcessor.java`
> 阅读建议：先看 [[06-生产者与消费者模型总览]] 和 [[07-客户端设计详解]]，再回来读这篇具体实现。

## 先给结论

- `RebalanceImpl` 在 5.5.0 里仍然是经典 Java Client 的消费分配骨架，但它已经不只服务传统 Pull/Push，也承接 broker 返回的 POP assignment。
- “Rebalance = 本地排序后平均分队列”只对一部分路径成立。5.5.0 里它有两条入口：本地 `rebalanceByTopic(...)`，以及 broker 参与的 `getRebalanceResultFromBroker(...)`。
- POP 没有绕开 `RebalanceImpl`。恰恰相反，POP 是通过 `MessageQueueAssignment.mode == POP` 进入 `updateMessageQueueAssignment(...)` 的。
- `RebalancePushImpl` 和 `RebalanceLitePullImpl` 的最大差别，不是算法，而是“队列变化后如何收尾、如何算初始 offset、如何派发请求”。
- `RebalanceService` 并不是固定 20 秒盲扫一次；不平衡时会退到更短的 `minInterval`。

## 不要把这篇和  混成一篇

 回答的是：

- 有哪些消费模型
- `PULL` / `POP` 是什么层的模式
- broker assignment 为什么会出现

本篇只回答：

- 经典 Java Client 收到这些模式后，具体怎样在本地维护 `ProcessQueue` / `PopProcessQueue`

也就是说：

- `67` 讲抽象边界
- `45` 讲客户端骨架落地

## RebalanceImpl 的两条主入口

`doRebalance(...)` 的核心分支是：

```java
if (!clientRebalance(topic)) {
    boolean result = this.getRebalanceResultFromBroker(topic, isOrder);
} else {
    boolean result = this.rebalanceByTopic(topic, isOrder);
}
```

所以源码里的问题不是“有没有 rebalance”，而是：

- 这次由客户端本地算
- 还是由 broker 先算 assignment 再下发

### 本地 rebalance 路径

`rebalanceByTopic(...)` 的 clustering 分支会：

1. [[06-生产者与消费者模型总览]]读 `topicSubscribeInfoTable`
2. [[07-客户端设计详解]]读 `findConsumerIdList(topic, consumerGroup)`
3. [[10-Offset管理机制]]对 `mqAll` / `cidAll` 排序
4. [[48-Pop消费模式详解]]调 `AllocateMessageQueueStrategy.allocate(...)`
5. [[53-5.x消费模型与负载均衡]]更新本地 `processQueueTable`

这是大家最熟悉的经典路径。

### broker assignment 路径

`getRebalanceResultFromBroker(...)` 会：

1. [[06-生产者与消费者模型总览]]调 `mQClientFactory.queryAssignment(...)`
2. [[07-客户端设计详解]]读回 `Set<MessageQueueAssignment>`
3. [[10-Offset管理机制]]按 `assignment.getMode()` 分成 push assignment 和 pop assignment
4. [[48-Pop消费模式详解]]进入 `updateMessageQueueAssignment(...)`

这说明 5.5.0 里客户端并不是永远自己分配；它已经能消费 broker 下发的模式化 assignment。

## RebalanceService 实际怎么调度

`RebalanceService` 默认参数是：

```java
waitInterval = 20000
minInterval = 1000
```

运行逻辑不是固定 20 秒一次，而是：

```java
boolean balanced = this.mqClientFactory.doRebalance();
realWaitInterval = balanced ? waitInterval : minInterval;
```

这意味着：

- 系统稳定时按 20s 慢速巡检
- 一旦发现不平衡，会加速到 1s 级别重试

所以“rebalance 周期就是 20 秒”是个不精确的说法。

## 本地 rebalance 不是只增不减

`updateProcessQueueTableInRebalance(...)` 先删后加。

### 先删

它会把以下 queue 标为 dropped 并进入移除候选：

- 已不属于本客户端的 queue
- `pq.isPullExpired()` 且当前是被动消费模式的 queue

然后调用：

```java
removeUnnecessaryMessageQueue(mq, pq)
```

这一步的真实含义是：

- rebalance 不是只算结果
- 还要负责旧 queue 的 offset 落盘、解锁、移除

### 再加

对于新分到的 queue，它会：

1. [[06-生产者与消费者模型总览]]`removeDirtyOffset(mq)`
2. [[07-客户端设计详解]]`createProcessQueue()`
3. [[10-Offset管理机制]]`computePullFromWhere(...)`
4. [[48-Pop消费模式详解]]构造 `PullRequest`
5. [[53-5.x消费模型与负载均衡]]`dispatchPullRequest(...)`

所以 `RebalanceImpl` 是“分配 + 队列状态迁移 + 初始消费位点计算 + 新请求派发”的整套骨架，不是单纯算法器。

## Push、LitePull、POP 的分歧点在哪里

### PushConsumer：RebalancePushImpl

`RebalancePushImpl` 的几个关键行为：

- `messageQueueChanged(...)` 会更新订阅版本 `subVersion`
- 会动态重算 `pullThresholdForQueue` / `pullThresholdSizeForQueue`
- 会 `sendHeartbeatToAllBrokerWithLockV2(true)` 通知 broker
- 顺序消费下移除 queue 时，会先持久化 offset，再尝试解锁 queue

这说明 PushConsumer 的 rebalance 不只是本地状态变化，它还会反向通知 broker，并处理顺序消费锁。

### LitePullConsumer：RebalanceLitePullImpl

LitePull 的实现更轻：

- `messageQueueChanged(...)` 只回调 `MessageQueueListener`
- `removeUnnecessaryMessageQueue(...)` 只做 offset persist/remove
- `dispatchPullRequest(...)` 是空实现
- `dispatchPopPullRequest(...)` 也是空实现

也就是说：

- LitePull 的 rebalance 主要负责“维护 assigned queue 集”
- 真正的拉取任务调度由 `DefaultLitePullConsumerImpl` 自己管理

### POP：仍然挂在 RebalanceImpl 下面

`updateMessageQueueAssignment(...)` 会把 assignment 分成两类：

- `mq2PushAssignment`
- `mq2PopAssignment`

对 push assignment：

- 创建 `ProcessQueue`
- 计算 `nextOffset`
- 派发 `PullRequest`

对 pop assignment：

- 创建 `PopProcessQueue`
- 构造 `PopRequest`
- 派发 `dispatchPopPullRequest(...)`

这说明 POP 不是“没有 rebalance”，而是“同一 rebalance 骨架里多了一条 pop 分支”。

## POP 还会反向影响订阅集

`updateMessageQueueAssignment(...)` 里有一个很容易漏掉的分支：

- 如果从 pop 切回 push，会重新订阅 pop retry topic
- 如果从 push 切到 pop，会取消该 retry topic 的普通订阅

对应逻辑是：

```java
final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
```

这说明 rebalance 影响的不是只有 queue 集合，还会影响：

- retry topic 订阅关系
- 后续失败消息的消费路径

这也是它和 POP retry 语义耦合的地方。

## 初始 offset 计算也因实现而异

### PushConsumer

`RebalancePushImpl.computePullFromWhereWithException(...)` 会根据 `ConsumeFromWhere` 决定：

- 读本地 / 远端 offset
- 首次启动时读 `maxOffset`
- retry topic 首次启动时从 `0` 或 `maxOffset` 开始

它还会区分：

- 普通 topic
- `%RETRY%` topic

### LitePullConsumer

`RebalanceLitePullImpl.computePullFromWhereWithException(...)` 逻辑相似，但读取 offset 的方式用的是：

```java
ReadOffsetType.MEMORY_FIRST_THEN_STORE
```

而且它没有 Push 那套 callback / orderly 语义收尾。

所以“RebalanceImpl 统一处理 offset”也不准确；真正的初始位点计算在各子类里。

## 顺序消费为什么需要额外 lock/unlock

`RebalancePushImpl` 在顺序消费且 clustering 模式下，会：

- 新增 queue 前先 `lock(mq)`
- 移除 queue 时尝试 `unlock(mq, true)`

这是因为顺序消费的正确性不是只靠 assignment 集合，而要靠 broker 侧 queue 锁配合。

所以：

- 顺序消费的 rebalance 成本更高
- 移除 queue 不一定立即成功
- `UNLOCK_DELAY_TIME_MILLS` 会影响释放时机

## 这篇要纠正的几个旧说法

- “Rebalance 就是本地平均分配。”  
  5.5.0 里还有 broker assignment 路径。

- “POP 没有 Rebalance。”  
  POP 通过 `MessageQueueAssignment.mode == POP` 进入同一个骨架。

- “RebalanceService 每 20 秒执行一次。”  
  不平衡时会退到 `minInterval=1000ms`。

- “队列变更只影响新拉取。”  
  实际还会影响 offset 落盘、顺序锁、retry topic 订阅与 pull/pop 请求派发。

## 建议连读顺序

1. [[06-生产者与消费者模型总览]][[06-生产者与消费者模型总览]]
2. [[07-客户端设计详解]]
3. [[10-Offset管理机制]]
4. [[48-Pop消费模式详解]]
5. [[53-5.x消费模型与负载均衡]]

## 相关笔记