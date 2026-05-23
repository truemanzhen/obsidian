# NameServer 路由设计

> 研究定位：把 NameServer 从“无状态注册中心”拆成内存路由表、Broker 注册入口、Topic 查询入口、失活清理与静态 Topic 映射承载面。
> 关键源码：`namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java`、`broker/src/main/java/org/apache/rocketmq/broker/out/BrokerOuterAPI.java`、`broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java`
> 阅读建议：先看本篇，再连读 [[66-Broker与Subscription管理]] 和 [[06-生产者与消费者模型总览]]。

## 先给结论

- NameServer 的核心职责不是保存 Broker 全量配置，而是维护一份可被客户端查询的路由视图。
- 这份路由视图的中心类是 `RouteInfoManager`，核心表包括 `topicQueueTable`、`brokerAddrTable`、`clusterAddrTable`、`brokerLiveTable`、`filterServerTable`。
- 5.5.0 里路由层已经开始承载静态 Topic 的逻辑队列映射，`topicQueueMappingInfoTable` 是新增的关键视图。
- 常规注册进入 NameServer 的主体是 Topic 配置包装，而不是 `SubscriptionGroupConfig`。
- NameServer 之间不做强一致同步，每个节点维护自己的内存路由快照，依赖 Broker 持续注册来收敛。

## NameServer 真正管理的是什么

从职责边界上看，NameServer 管理的是：

- Topic 到 QueueData 的映射
- brokerName 到 brokerAddr 的映射
- cluster 到 brokerName 的映射
- broker 存活状态
- filter server 列表

它不管理：

- 消息数据
- 消费 offset
- 消费组运行状态
- Broker 本地所有配置项

所以研究 NameServer 时，正确问题不是“它保存了什么元数据”，而是“它保存了哪些路由可见元数据”。

## `RouteInfoManager` 的几张主表

### `topicQueueTable`

```java
Map<String/* topic */, Map<String, QueueData>> topicQueueTable
```

Topic 到 broker 维度 `QueueData` 的映射。

### `brokerAddrTable`

```java
Map<String/* brokerName */, BrokerData> brokerAddrTable
```

BrokerData 里带的是 brokerName、cluster 和 `brokerId -> brokerAddr`。

### `clusterAddrTable`

```java
Map<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable
```

表达集群拓扑归属。

### `brokerLiveTable`

保存最后心跳时间、超时时间、channel 等活跃信息。

### `filterServerTable`

保存 broker 对应 filter server 列表。

### `topicQueueMappingInfoTable`

这是 5.5.0 里需要特别注意的扩展表，用来承载静态 Topic 的逻辑队列映射信息。

## Broker 注册到底向 NameServer 送了什么

`BrokerController.registerBrokerAll(...)` 会构造：

```java
TopicConfigAndMappingSerializeWrapper topicConfigWrapper = ...
this.brokerOuterAPI.registerBrokerAll(..., topicConfigWrapper, ...)
```

`BrokerOuterAPI.registerBrokerAll(...)` 进一步放入请求体的是：

```java
RegisterBrokerBody requestBody = new RegisterBrokerBody();
requestBody.setTopicConfigSerializeWrapper(...);
requestBody.setFilterServerList(...);
```

也就是说，常规注册主体是：

- topic 配置
- topic 队列映射
- filter server 列表

而不是消费组表。

这点和  要形成闭环。

## `registerBroker(...)` 真正更新了哪些视图

`RouteInfoManager.registerBroker(...)` 的主逻辑可以概括成：

1. 更新 `clusterAddrTable`
2. 更新 `brokerAddrTable`
3. 更新 `brokerLiveTable`
4. 对 master 或特定 slave，把 `topicConfigWrapper` 折算进 `topicQueueTable`
5. 维护 `filterServerTable`
6. 必要时维护 `topicQueueMappingInfoTable`

所以 Broker 注册不是单纯“报个活”，而是“把路由图谱刷新到当前版本”。

## Topic 查询真正返回的是什么

客户端查询 Topic 路由时，关键入口是：

```java
pickupTopicRouteData(final String topic)
```

它最终会组装：

- `queueDatas`
- `brokerDatas`
- `filterServerTable`
- `topicQueueMappingByBroker`

因此客户端拿到的不是一张简单的 topic->broker 字典，而是一份完整的 `TopicRouteData` 快照。

## 为什么说静态 Topic 让路由层变复杂了

普通路由里，逻辑 topic 往往还能被近似理解成：

- topic
- brokerName
- queueId

但 5.5.0 的静态 Topic 路由会额外携带：

```java
topicRouteData.setTopicQueueMappingByBroker(this.topicQueueMappingInfoTable.get(topic));
```

这意味着：

- 逻辑 queueId 不再必然等于物理 queueId
- 客户端后续还要结合映射信息进行重写

所以 NameServer 在 5.5.0 里已经不只是“普通 Topic 路由表”，还承载了一层逻辑映射面。

## NameServer 的失活清理是被动收缩

`scanNotActiveBroker()` 会扫描 `brokerLiveTable`，一旦超过超时时间：

- 关闭 channel
- 触发 `onChannelDestroy(...)`
- 清理对应 Broker 的路由信息

所以：

- 路由不是持久不变的静态缓存
- 它会随着 Broker 心跳和注册不断收缩 / 扩张

## NameServer 的一致性模型是什么

它不是：

- 多节点强同步
- 主从复制
- 共识选举

而是：

- 每个节点各自维护一份内存路由快照
- Broker 向多个 NameServer 重复注册
- 客户端从任一 NameServer 拉取路由

所以更准确的描述是：

- 路由视图最终收敛
- 不是 NameServer 集群内部做实时强一致

## 这篇要纠正的几个旧说法

- “NameServer 存的是 Broker 全量配置。”  
  不对，它维护的是路由可见视图。

- “订阅组和 Topic 一起走常规路由注册。”  
  不对，常规注册核心是 Topic 配置包装。

- “NameServer 节点之间会互相同步路由。”  
  不对，各节点独立，靠 Broker 持续注册收敛。

- “路由就是 topic 对 broker 的简单映射。”  
  不对，还包括 QueueData、BrokerData、FilterServer 和静态映射信息。

## 建议连读顺序

1. [[66-Broker与Subscription管理]]
2. [[06-生产者与消费者模型总览]]
3. [[07-客户端设计详解]]
4. [[42-StaticTopic逻辑队列映射]]
