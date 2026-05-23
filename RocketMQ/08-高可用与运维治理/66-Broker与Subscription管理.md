# Broker 与 Subscription 管理

> 研究定位：把 Broker 侧元数据治理拆成 `TopicConfigManager`、`SubscriptionGroupManager`、NameServer 路由注册、自动创建、重试主题衍生和运维管理边界。
> 关键源码：`broker/src/main/java/org/apache/rocketmq/broker/topic/TopicConfigManager.java`、`broker/src/main/java/org/apache/rocketmq/broker/subscription/SubscriptionGroupManager.java`、`broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java`、`broker/src/main/java/org/apache/rocketmq/broker/out/BrokerOuterAPI.java`、`namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java`
> 阅读建议：先区分“Broker 本地配置”和“NameServer 路由视图”，这两者不是一份数据。

## 先给结论

- RocketMQ 5.5.0 里，Topic 和 SubscriptionGroup 都由 Broker 本地管理，但只有 Topic 路由信息会作为常规注册体直接进入 NameServer 路由视图。
- `SubscriptionGroupConfig` 不是 NameServer 正常路由注册的数据主体；把“订阅组和 Topic 一起同步到 NameServer”当成常规结论是不准确的。
- Topic 元数据核心在 `TopicConfigManager`，消费组元数据核心在 `SubscriptionGroupManager`，两者职责边界是清晰分开的。
- 自动创建 Topic 和自动创建 SubscriptionGroup 都仍然存在，但触发入口、约束条件和副作用不一样。
- “最大重试次数默认 16 次”只能视为 `SubscriptionGroupConfig.retryMaxTimes` 的默认值，不能直接外推成所有消费模型和所有客户端的统一行为结论。
- 重试主题、DLQ、POP retry topic 都依赖 Topic / Group 元数据，但它们的创建触发点并不相同。

## 先把三种“元数据视图”分开

### 1. Broker 本地配置视图

这是 Broker 自己持有和持久化的配置：

- `topicConfigTable`
- `subscriptionGroupTable`
- `forbiddenTable`

### 2. NameServer 路由视图

NameServer 常规关心的是：

- cluster
- brokerName / brokerAddr
- topicQueueData
- filter server 等路由相关信息

### 3. 客户端运行时视图

客户端真正看到的是：

- topic route
- group 是否存在
- 某些管理命令返回的 broker 本地配置

如果不先把这三层拆开，就很容易误把“Broker 本地存在某配置”当成“NameServer 路由一定可见”。

## `TopicConfigManager` 负责 Broker 侧 Topic 元数据

主表就是：

```java
ConcurrentMap<String, TopicConfig> topicConfigTable
```

它在 `init()` 阶段会预置一批系统 topic，例如：

- `TBW102`
- `SCHEDULE_TOPIC_XXXX`
- trace topic
- reply topic
- 事务相关 topic
- `REVIVE_LOG_<cluster>`

其中 revive topic 的初始化还明确用了：

```java
PopAckConstants.buildClusterReviveTopic(brokerClusterName)
```

所以：

- 5.5.0 的 revive topic 是 cluster 级
- 不是某些旧资料里写的按 clientId 派生

## Topic 创建有多条入口，不要只记一种

### 1. 普通发送触发自动建 Topic

`createTopicInSendMessageMethod(...)` 是业务发送路径常见入口。

它会基于默认 topic 继承出新 topic：

```java
TopicConfig defaultTopicConfig = getTopicConfig(defaultTopic);
...
topicConfig.setReadQueueNums(queueNums);
topicConfig.setWriteQueueNums(queueNums);
topicConfig.setPerm(perm & ~PermName.PERM_INHERIT);
```

这条路径回答的是：

- “业务 Topic 不存在时，发送链路如何自动补建”

### 2. send-back / retry / DLQ 衍生 Topic

`createTopicInSendMessageBackMethod(...)` 则服务于：

- `%RETRY%group`
- `%DLQ%group`
- 其他 send-back 场景衍生主题

这条路径和普通业务 topic 继承默认 topic 的语义不同，它是“Broker 内部运维/重试资源补建”。

## Topic 变更的关键副作用是更新版本并注册 Broker 数据

无论创建还是更新，最终都伴随：

- `updateDataVersion()`
- `persist()`
- `registerBrokerData(topicConfig)` 或 `registerBrokerAll(...)`

这说明 Topic 元数据不是“只改本地文件”。

它还要进一步驱动：

- NameServer 侧路由刷新
- Broker 之间的注册传播

## `SubscriptionGroupManager` 负责消费组元数据

主表和附加表分别是：

```java
ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable
ConcurrentMap<String, ConcurrentMap<String, Integer>> forbiddenTable
```

这里要注意一个容易看反的点：

- `forbiddenTable` 的第一层 key 是 group
- 第二层 key 才是 topic

所以它表达的是：

- 某个 group 对某些 topic 的禁止位图配置

## `SubscriptionGroupConfig` 不只是重试次数

研究时至少要关注这些字段：

- `consumeEnable`
- `consumeBroadcastEnable`
- `retryQueueNums`
- `retryMaxTimes`
- `groupRetryPolicy`
- 各类 attributes

其中最容易被旧资料简化错的是：

- `retryMaxTimes = 16`

源码里这只是默认字段值，不是你文档里应该直接写成“RocketMQ 一定 16 次后死信”的总规则。

更准确的说法是：

- `SubscriptionGroupConfig` 默认把 `retryMaxTimes` 设成 16
- 经典 send-back 链路会读取它
- 但 POP / gRPC / receipt renew 并不等价于这条老路径

## 消费组自动创建也仍然存在

`findSubscriptionGroupConfig(...)` 的逻辑是：

```java
if (subscriptionGroupConfig == null) {
    if (autoCreateSubscriptionGroup || sysGroupAllowed) {
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(group);
        putSubscriptionGroupConfigIfAbsent(subscriptionGroupConfig);
        updateDataVersion();
        persist();
    }
}
```

也就是说：

- 普通 group 不存在时，是否自动创建仍然受 Broker 配置控制
- 不是“必须先手工创建 group”
- 也不是“所有 group 都会无条件自动出现”

## Topic 与 Group 的运维边界不一样

### Topic

- 会影响 NameServer 路由
- 会影响发送/拉取路径是否可解析
- 常由发送链、管理命令、内部系统流程触发创建

### SubscriptionGroup

- 主要约束消费与重试行为
- 不属于 NameServer 常规路由主体
- 常由消费链、管理命令、自动创建逻辑触发

这也是为什么把 Topic 和 SubscriptionGroup 直接写成“同类元数据，一起同步到 NameServer”会模糊掉关键边界。

## Broker 常规注册到 NameServer 的主体是什么

`BrokerController.registerBrokerAll(...)` 会构造：

```java
TopicConfigAndMappingSerializeWrapper topicConfigWrapper = ...
this.brokerOuterAPI.registerBrokerAll(..., topicConfigWrapper, ...)
```

`BrokerOuterAPI.registerBrokerAll(...)` 里进一步放进请求体的是：

```java
RegisterBrokerBody requestBody = new RegisterBrokerBody();
requestBody.setTopicConfigSerializeWrapper(...)
requestBody.setFilterServerList(...)
```

NameServer `RouteInfoManager.registerBroker(...)` 的签名里接收的也是：

```java
TopicConfigSerializeWrapper topicConfigWrapper
```

从这条链路就能看出：

- 常规注册体核心是 Topic 配置与队列映射
- 不是 `SubscriptionGroupTable`

所以你的研究笔记里如果写“Broker 定时把订阅组同步到 NameServer 路由”，需要明确标成不准确甚至应删除。

## 订阅组信息并不是完全不出 Broker，只是路径不同

虽然它不在常规路由注册体里，但 `BrokerOuterAPI` 仍有获取订阅组包装的能力，例如：

- `SubscriptionGroupWrapper`

这类能力更多是：

- broker 间同步
- 管理面拉取
- 特定治理用途

而不是 NameServer 常规路由注册主线。

这点必须写清，不然读者很容易把“有远程获取能力”误读成“它属于正常路由注册内容”。

## Retry / DLQ / POP retry 依赖 Topic 与 Group，但触发点不同

### 经典 retry / DLQ

主要依赖：

- `SubscriptionGroupConfig.retryQueueNums`
- `SubscriptionGroupConfig.retryMaxTimes`
- `TopicConfigManager.createTopicInSendMessageBackMethod(...)`

### POP retry

则更多依赖：

- `KeyBuilder.buildPopRetryTopic(...)`
- POP 主链与 revive 逻辑

它和经典 `%RETRY%group` 不属于同一资源模型。

所以这一篇里对“Subscription 管理”的正确补充是：

- 它不仅影响消费权限
- 还间接决定 retry / DLQ 类资源如何被使用

但不要反过来写成“所有重试主题都只是订阅组的一种普通属性投影”。

## Topic 删除和 Group 删除的后果也不同

### 删除 Topic

`deleteTopicConfig(...)` 会：

- 移除本地 topic 配置
- 更新 dataVersion
- 持久化

随后还会影响路由暴露与消息收发。

### 删除 Group

`deleteSubscriptionGroupConfig(...)` 会：

- 删除 group 配置
- 删除 group 对应 forbidden 配置
- 更新版本并持久化

它影响的是：

- 消费校验
- retry / offset / 管理命令约束

不是像 Topic 删除那样直接改变 NameServer 路由拓扑。

## 这篇要纠正的几个旧说法

- “Topic 和 SubscriptionGroup 都由 Broker 定时同步到 NameServer 路由。”  
  常规 `registerBrokerAll(...)` 注册主体是 topic 配置包装，不是 subscription group 表。

- “最大重试次数就是固定 16 次。”  
  16 只是 `retryMaxTimes` 默认值，且不同消费模型路径不应直接套同一口径。

- “Group 只是消费权限配置。”  
  它还影响 retry queue 数、重试策略、自动创建与禁止表治理。

- “Broker 元数据就是一张总表。”  
  Topic、SubscriptionGroup、forbidden、路由注册视图是分层管理的。

## 建议连读顺序

1. [[02-NameServer路由设计]]
2. [[04-Topic与消息类型约束]]
3. [[11-消费重试与死信队列]]
4. [[48-Pop消费模式详解]]
5. [[65-ACL认证授权]]

## 相关笔记

- [[02-NameServer路由设计]]
- [[04-Topic与消息类型约束]]
- [[11-消费重试与死信队列]]