# Offset 管理机制

> 研究定位：把 RocketMQ 5.5.0 的 offset 体系拆成客户端本地缓存、广播/集群持久化、Broker 侧 `ConsumerOffsetManager`、静态队列映射修正和历史回溯边界。
> 关键源码：`client/src/main/java/org/apache/rocketmq/client/consumer/store/RemoteBrokerOffsetStore.java`、`client/src/main/java/org/apache/rocketmq/client/consumer/store/LocalFileOffsetStore.java`、`broker/src/main/java/org/apache/rocketmq/broker/offset/ConsumerOffsetManager.java`、`broker/src/main/java/org/apache/rocketmq/broker/processor/ConsumerManageProcessor.java`
> 阅读建议：先把“客户端手里的 offset”和“Broker 当前仍承认的 offset”分开，再连读 [[33-消息存储清理与回溯边界]]。

## 先给结论

- RocketMQ 里的 offset 不是单一存储点，而是“客户端缓存 + 持久化载体 + Broker 校验逻辑”的组合系统。
- 集群消费下，客户端通常通过 `RemoteBrokerOffsetStore` 把 offset 回写 broker；广播消费下则走 `LocalFileOffsetStore` 本地文件。
- Broker 侧真实持久化核心是 `ConsumerOffsetManager`，主表结构是 `topic@group -> queueId -> offset`。
- `ConsumerOffsetManager` 不只维护消费 offset，还额外维护了 `pullOffsetTable` 和 `resetOffsetTable`。
- “没有找到 offset 就从 0 开始”是过度简化。Broker 的查询返回还受 `setZeroIfNotFound`、当前最小 offset、是否仍有数据窗口等条件影响。
- 即使 offset 记录还在，消息也可能已经因为存储清理而不可回溯。offset 语义必须和 [[33-消息存储清理与回溯边界]] 一起看。

## 先分清三层 offset

### 1. [[06-生产者与消费者模型总览]]客户端内存 offset

无论本地文件还是远端 broker，客户端都会先在内存 `offsetTable` 里维护当前视图。

### 2. [[07-客户端设计详解]]客户端持久化 offset

- 广播模式：本地 `offsets.json`
- 集群模式：远端 broker

### 3. Broker 持久化 offset

Broker 最终以 `ConsumerOffsetManager` 为准，把 group-topic-queue 的消费进度持久化到配置文件。

研究时如果不把这三层分开，就很容易把“客户端当前看到的值”和“broker 仍认可的值”混成一回事。

## 客户端为什么要先维护一份本地内存 offset

无论 `LocalFileOffsetStore` 还是 `RemoteBrokerOffsetStore`，`updateOffset(...)` 都先更新本地 map。

例如：

```java
ControllableOffset offsetOld = this.offsetTable.get(mq);
...
offsetOld.update(offset);
```

这意味着：

- 消费线程、rebalance、拉取逻辑读到的是客户端自己的即时视图
- 持久化动作是后续批量同步，而不是每处理一条消息就立刻远端写入

所以 offset 天然存在“内存值先变化，持久化稍后发生”的时间差。

## 广播模式：本地文件才是持久化主载体

`LocalFileOffsetStore` 默认路径是：

```java
System.getProperty("user.home") + File.separator + ".rocketmq_offsets"
```

最终具体文件形态是：

```text
{clientId}/{group}/offsets.json
```

它的读取逻辑是：

```java
case MEMORY_FIRST_THEN_STORE:
case READ_FROM_MEMORY:
case READ_FROM_STORE:
```

`persistAll(...)` 则会把当前活跃 queue 的 offset 写回本地 JSON。

所以广播模式下最关键的事实是：

- 每个客户端实例自己保存消费进度
- 并不存在一个 group 级共享 offset

## 集群模式：远端 broker 才是主持久化面

`RemoteBrokerOffsetStore.readOffset(...)` 的核心分支是：

```java
long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
this.updateOffset(mq, brokerOffset, false);
return brokerOffset;
```

而 `persistAll(...)` 最终会：

```java
this.updateConsumeOffsetToBroker(mq, offset.getOffset());
```

这说明集群模式的真实语义是：

1. [[06-生产者与消费者模型总览]]客户端先维护一份本地内存 offset
2. [[07-客户端设计详解]]定期把活跃 queue 的 offset 写回 broker
3. broker 持久化后，后续新客户端或重平衡时再从 broker 查回

因此：

- 集群 offset 不是“只存在 broker”
- 也不是“客户端重启前完全不管 offset”

## `ConsumerOffsetManager` 才是 Broker 侧 offset 核心

Broker 主表结构就是：

```java
ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable
```

并且还有两张旁路表：

```java
resetOffsetTable
pullOffsetTable
```

### `offsetTable`

记录正常消费进度。

### `pullOffsetTable`

记录 pull 语义下最近拉取进度；`queryPullOffset(...)` 查不到时会回退到 `queryOffset(...)`。

### `resetOffsetTable`

在开启 `useServerSideResetOffset` 时，优先覆盖正常消费 offset 的查询结果。

所以 offset 在 Broker 上从来就不是只有一张“最终表”。

## Broker 查询 offset 的优先级

`ConsumerOffsetManager.queryOffset(...)` 的核心逻辑是：

```java
if (isUseServerSideResetOffset()) {
    if (resetOffsetTable contains queueId) {
        return resetOffset;
    }
}
return offsetTable.get(...)
```

这意味着一旦服务端 reset offset 生效，客户端查询到的值不一定是原始消费进度，而可能是临时覆盖值。

所以：

- “Broker 里就一个 offset 值”是错的
- reset 是查询层优先级覆盖，不只是管理工具层面的概念

## Broker 怎么接收和返回 offset

真正处理 remoting 请求的是 `ConsumerManageProcessor`。

### 更新 offset

`UPDATE_CONSUMER_OFFSET` 会做这些检查：

- group 是否存在
- topic 是否存在
- queueId / offset 是否为空
- 若启用了 server-side reset 且该 queue 仍有 reset 标记，则拒绝本次更新

最终才会：

```java
consumerOffsetManager.commitOffset(...)
```

### 查询 offset

`QUERY_CONSUMER_OFFSET` 先查：

```java
consumerOffsetManager.queryOffset(group, topic, queueId)
```

如果查不到，再结合：

- `setZeroIfNotFound`
- 当前 queue `minOffset`
- `checkInMemByConsumeOffset(...)`

决定返回：

- `SUCCESS + offset`
- `QUERY_NOT_FOUND`

这也是为什么“查不到 offset 就直接返回 0”并不准确。

## 为什么“首次消费从哪开始”不能简单写成 offset=0

如果 Broker 查不到记录，`ConsumerManageProcessor.queryConsumerOffset(...)` 的逻辑并不是无脑给 0。

只有在一些特定条件下才可能返回 0：

```java
minOffset <= 0
&& checkInMemByConsumeOffset(topic, queueId, 0, 1)
```

否则它可能直接返回：

- `QUERY_NOT_FOUND`

然后由客户端结合自己的 `ConsumeFromWhere` 和实现分支决定后续起点。

更完整的初始位点计算是在各类 `RebalanceImpl` 子类里完成的，不能只靠 Broker 这一段逻辑下结论。

## Offset 和静态 Topic / 逻辑队列映射已经耦合

`ConsumerManageProcessor` 里对静态 Topic 有两段很重要的改写逻辑：

- `rewriteRequestForStaticTopic(...)`
- `rewriteResponseForStaticTopic(...)`

它说明在静态队列映射场景下：

- 客户端传来的逻辑 queueId / 逻辑 offset
- 不一定就是 Broker 最终落盘和查询时的物理 queueId / 物理 offset

因此：

- offset 语义在 5.5.0 已经不是“queueId + long”这么朴素
- 它会受到逻辑队列映射层的改写

## Broker 还会主动清理部分 offset

`ConsumerOffsetManager.scanUnsubscribedTopic()` 会扫描：

- 当前 group 是否还订阅该 topic
- offset 是否已经落到 `minOffsetInStore` 之后很久

满足条件时，会移除该 `topic@group` 的 offset 项。

这意味着 offset 不是“只增不删”的永恒记录。

尤其在：

- group 不再订阅
- 数据窗口早已前移

时，Broker 会主动做治理收缩。

## 广播 offset 在 Broker 侧还有一条辅助分支

5.5.0 里除了客户端本地 `LocalFileOffsetStore`，Broker 还存在：

- `BroadcastOffsetManager`
- `BroadcastOffsetStore`

它主要服务于广播消费 / Proxy 协同等场景的辅助 offset 管理。

但研究主线里要抓住主次：

- 广播消费的第一持久化面仍是客户端本地文件
- Broker 广播 offset 管理是附加能力，不要喧宾夺主

## LMQ 还有专门的 offset 派生实现

源码里还有：

- `LmqConsumerOffsetManager`

它是 `ConsumerOffsetManager` 的 LMQ 变体。

所以如果后续你继续深挖 LiteTopic / LMQ，要记住：

- offset 体系在那条支线上也做了专项扩展
- 但主语义仍然继承自 broker offset 管理骨架

## Offset 记录不等于消息仍可回溯

这是研究里最容易踩的坑。

即使：

- Broker 里还能查到 offset
- 客户端也还记得旧值

消息仍可能因为 CommitLog / ConsumeQueue 清理而读不到。

也就是说：

- offset 是“消费位置记录”
- 不是“历史消息保留承诺”

这部分必须和  联动理解。

## 这篇要纠正的几个旧说法

- “Offset 就存在 Broker 一处。”  
  实际至少有客户端内存、本地文件或 broker 持久化三个层次。

- “广播模式也主要看 Broker offset。”  
  广播主持久化面是客户端本地 `offsets.json`。

- “查不到 offset 就从 0 开始。”  
  还要看 `setZeroIfNotFound`、`minOffset`、是否仍有数据窗口以及客户端起始策略。

- “offset 还在，就一定能回溯到老消息。”  
  存储清理后，offset 记录和消息保留窗口可以脱钩。

## 建议连读顺序

1. [[06-生产者与消费者模型总览]]
2. [[07-客户端设计详解]]
3. [[11-消费重试与死信队列]]
4. [[09-客户端Rebalance实现]]
5. [[48-Pop消费模式详解]]

## 相关笔记

- [[11-消费重试与死信队列]]