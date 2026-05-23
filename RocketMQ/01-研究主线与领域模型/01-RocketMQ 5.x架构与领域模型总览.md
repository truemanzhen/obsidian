# RocketMQ 5.x 架构与领域模型总览

> 研究定位：先建立 5.5.0 的对象模型、模块边界和主链路，再下钻具体实现。
> 关键源码：`namesrv/`、`broker/`、`proxy/`、`client/`、`store/`、`common/`
> 阅读建议：这篇只负责立框架，读完立刻接 [[02-NameServer路由设计]] 与 [[03-消息协议与类体系]]。

## 为什么先读这一篇

- 5.5.0 已经不是“NameServer + Broker + Java Client”这一层老模型。
- 新版源码里同时出现了 `proxy`、`grpc/v2`、`TopicMessageType`、`MessageRequestMode`、`LiteTopic`、`RocksDB`、`TieredStore`。
- 如果直接钻具体类，很容易只看到局部链路，看不到系统到底在管理哪些对象、通过哪些契约协作。

## 先给结论

- RocketMQ 的控制面核心是 `Topic`、`ConsumerGroup`、`RouteData`、`TopicConfig`、`SubscriptionGroupConfig`。
- RocketMQ 的数据面核心是 `Producer -> Proxy/Remoting -> BrokerProcessor -> CommitLog -> ConsumeQueue/Index -> Consumer`。
- RocketMQ 的 5.x 增量，不只是“多了几个功能点”，而是把消息类型、消费模式、LiteTopic、存储后端这些概念显式建模了。

## 5.5.0 的一级对象

| 对象 | 作用 | 关键类 |
|---|---|---|
| Topic | 写入 / 读取的逻辑资源 | `TopicConfig`、`TopicRouteData` |
| ConsumerGroup | 消费身份、重试和消费策略的承载体 | `SubscriptionGroupConfig` |
| Message | 业务数据和消息属性 | `Message`、`MessageExt`、`MessageBatch` |
| Queue | Topic 下的读写分片 | `MessageQueue`、`QueueData` |
| TopicMessageType | Topic 能接受的消息语义 | `TopicMessageType` |
| MessageRequestMode | Topic + Group 维度的消费请求模式 | `MessageRequestModeManager` |
| Namespace | 资源命名隔离 | `NamespaceUtil` |
| LiteTopic | 5.5.0 的轻量子主题模型 | `LiteUtil`、`LiteSubscriptionRegistry` |
| Broker | 处理、存储、治理的核心节点 | `BrokerController` |
| Proxy | 5.x gRPC 接入与协议适配层 | `GrpcServer`、`ClientActivity`、`SendMessageActivity` |

## 从源码角度看 5.5.0 的三条主线

### 1. 数据主线

```text
Producer / Consumer
        ↓
Proxy(gRPC) / Remoting(Java Client)
        ↓
Broker Processor
        ↓
CommitLog
        ↓
ConsumeQueue / Index / Timer / Retry
        ↓
Consumer Ack / Retry / Rebalance
```

### 2. 元数据主线

```text
TopicConfig
SubscriptionGroupConfig
TopicRouteData
MessageRequestMode
TopicMessageType
Namespace
```

### 3. 治理主线

```text
HA / DLedger / EscapeBridge
ACL / Metrics / mqadmin
RocksDB / TieredStore / ColdData
```

## 5.5.0 最值得单独建模的增量

- `TopicMessageType`：把 FIFO / DELAY / TRANSACTION / PRIORITY / LITE 的契约从约定提升为 topic 属性。
- `MessageRequestMode`：把 `PULL` / `POP` 的选择从客户端私有逻辑提升为 Broker 侧可持久化配置。
- `LiteTopic`：基于 LMQ 的轻量子主题模型，和 `LitePullConsumer` 不是一回事。
- `Proxy` / `gRPC v2`：5.x 官方新接入面，很多新语义首先在这里体现。
- `RocksDB` / `TieredStore`：不再只是存储优化，而是会影响消费边界、冷热读取和清理逻辑。

## 读源码时建议带着的四个问题

- `TopicMessageType` 只是路由提示，还是发送约束？
- `MessageRequestMode` 到底是客户端能力，还是 Broker 侧的 group 策略？
- `LitePullConsumer` 和 `LiteTopic` 为什么共用 “Lite” 这个名字，但本质不是一个层面的抽象？
- `Proxy` 在 5.x 中是可选接入层，还是新语义的主入口？

## 建议阅读顺序

1. 先读 [[02-NameServer路由设计]]，建立 Topic / Broker / Queue 的元数据关系。
2. 再读 [[03-消息协议与类体系]] 和 [[04-Topic与消息类型约束]]，补齐消息和 topic 的契约。
3. 然后读 [[06-生产者与消费者模型总览]]，再下钻 [[08-客户端Producer内部实现]]、[[09-客户端Rebalance实现]]。
4. 进入链路时，先走 [[13-Remoting网络层设计]] / [[22-消息处理链路总览]]，再看 [[23-SendMessageActivity]]。
5. 主链路通了，再看 [[34-延迟消息设计]]、[[48-Pop消费模式详解]] 这些 5.x 专题。

## 相关笔记