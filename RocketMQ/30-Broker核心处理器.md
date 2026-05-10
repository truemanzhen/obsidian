# Broker 核心处理器

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/processor/`

## 🏗️ 处理器架构

Broker 侧的处理器是**实际的消息处理引擎**，Proxy 层最终会转发到这些处理器。

```
┌─────────────────────────────────────────────────────────┐
│                    BrokerController                      │
│  ┌────────────────────────────────────────────────────┐ │
│  │              processorTable (RequestCode → Processor)│ │
│  ├──────────────┬──────────────┬──────────────────────┤ │
│  │SendMessage   │PullMessage   │QueryMessage          │ │
│  │Processor     │Processor     │Processor             │ │
│  ├──────────────┼──────────────┼──────────────────────┤ │
│  │PopMessage    │AckMessage    │ChangeInvisibleTime   │ │
│  │Processor     │Processor     │Processor             │ │
│  ├──────────────┼──────────────┼──────────────────────┤ │
│  │EndTransaction│AdminBroker   │ConsumerManage        │ │
│  │Processor     │Processor     │Processor             │ │
│  ├──────────────┼──────────────┼──────────────────────┤ │
│  │ClientManage  │QueryAssignment│Notification          │ │
│  │Processor     │Processor      │Processor             │ │
│  └──────────────┴──────────────┴──────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心处理器一览

| 处理器 | RequestCode | 职责 |
|---|---|---|
| `SendMessageProcessor` | SEND_MESSAGE / SEND_MESSAGE_V2 | 消息写入 |
| `PullMessageProcessor` | PULL_MESSAGE | 拉取消息 |
| `PopMessageProcessor` | POP_MESSAGE | Pop 模式消费（5.x 新） |
| `AckMessageProcessor` | ACK_MESSAGE / BATCH_ACK | Pop 模式确认 |
| `ChangeInvisibleTimeProcessor` | CHANGE_MESSAGE_INVISIBLETIME | 修改 Pop 消息可见时间 |
| `EndTransactionProcessor` | END_TRANSACTION | 事务消息提交/回滚 |
| `QueryMessageProcessor` | QUERY_MESSAGE / VIEW_MESSAGE_BY_ID | 消息查询 |
| `AdminBrokerProcessor` | 多种 ADMIN_* | 运维管理命令 |
| `ConsumerManageProcessor` | QUERY_CONSUMER_OFFSET / UPDATE_CONSUMER_OFFSET | Offset 管理 |
| `ClientManageProcessor` | HEART_BEAT / UNREGISTER_CLIENT / GET_CONSUMER_LIST_BY_GROUP | 客户端管理 |
| `QueryAssignmentProcessor` | QUERY_ASSIGNMENT | 队列分配查询 |

## 📤 SendMessageProcessor（消息发送）

### 类继承关系

```
AbstractSendMessageProcessor
  └── SendMessageProcessor
```

### 处理流程

```
1. processRequest(ctx, request)
   ├── 解析 SendMessageRequestHeader
   ├── 构建 MessageExtBrokerInner
   ├── 前置检查（Topic 是否存在、权限、消息大小）
   ├── 处理 TopicQueueMappingContext（逻辑队列映射）
   └── sendMessage(ctx, request, response, msgInner, ...)

2. sendMessage(...)
   ├── 处理重试消息（消费失败重投）
   ├── 处理延迟消息（设置延迟级别）
   ├── 处理事务消息（半消息标记）
   ├── 内部转发（Inner 天然支持）
   └── handleMessage(...)
       ├── brokerController.getMessageStore().putMessage(msgInner)
       ├── 处理 PutMessageResult
       │   ├── PUT_OK → 返回成功
       │   ├── FLUSH_DISK_TIMEOUT → 异步刷盘超时
       │   ├── FLUSH_SLAVE_TIMEOUT → 同步双写超时
       │   └── SERVICE_NOT_AVAILABLE → Broker 不可用
       └── 更新统计指标
```

### 关键设计

**1. 消息类型路由**

```java
// 根据消息类型选择不同处理路径
TopicMessageType topicMessageType = TopicMessageType.parseFromMessage(msg);
switch (topicMessageType) {
    case DELAY:
        // 延迟消息处理
        break;
    case TRANSACTION:
        // 事务消息处理
        break;
    case FIFO:
        // 顺序消息处理
        break;
    default:
        // 普通消息
}
```

**2. 逻辑队列映射（TopicQueueMapping）**

5.x 支持逻辑队列 → 物理队列的映射，用于静态 Topic 配置：
```java
TopicQueueMappingContext mappingContext = buildTopicQueueMappingContext(request, msg);
if (mappingContext.isRedirect()) {
    // 需要转发到其他 Broker
}
```

**3. RecalledMessage（消息撤回）**

5.5.0 新增消息撤回功能：
```java
// 生成撤回 Handle
RecallMessageHandle handle = new RecallMessageHandle(topic, msgId, storeTime);
// 客户端可以通过 handle 撤回已发送的消息
```

## 📥 PullMessageProcessor（消息拉取）

### 处理流程

```
1. processRequest(ctx, request)
   ├── 解析 PullMessageRequestHeader
   ├── 权限检查（Topic 读权限）
   ├── 消费组校验
   ├── 消息过滤器构建
   │   ├── ExpressionMessageFilter（Tag / SQL92）
   │   └── ExpressionForRetryMessageFilter（重试消息过滤）
   ├── 调用 messageStore.getMessage(...)
   └── 处理 GetMessageResult
       ├── FOUND → 直接返回消息
       ├── NO_MATCHED_MESSAGE → 无匹配消息
       ├── NO_NEW_MSG → 没有新消息
       ├── OFFSET_OVERFLOW_BAD_OFFSET → Offset 越界
       └── MESSAGE_WAS_REMOVING → 消息已被清理

2. 长轮询处理
   ├── 如果没有新消息且支持长轮询
   ├── 创建 PullRequest 加入 PullRequestHoldService
   └── 有新消息时唤醒等待的请求
```

### 关键设计

**1. 消息过滤（两级过滤）**

```java
// Broker 侧过滤
MessageFilter messageFilter = new ExpressionMessageFilter(
    subscriptionData.getSubString(),     // Tag 表达式
    subscriptionData.getExpressionType(), // TAG 或 SQL92
    consumerFilterData                    // 消费者过滤数据
);

// Store 层过滤
GetMessageResult result = messageStore.getMessage(
    group, topic, queueId, offset, maxMsgNums, messageFilter
);
```

**2. 长轮询集成**

```java
// 没有消息时挂起请求
if (getResult.getStatus() == GetMessageStatus.NO_NEW_MSG) {
    if (brokerController.getBrokerConfig().isLongPollingEnable()) {
        PullRequest pullRequest = new PullRequest(...);
        this.brokerController.getPullRequestHoldService().suspendPullRequest(pullRequest);
        response.setSuspended(true);
    }
}
```

## 📨 PopMessageProcessor（Pop 消费）

5.x 引入的全新消费模式，替代传统的 Push/Pull。

### 核心特点

1. **服务端驱动**：Broker 主动推送，不再依赖客户端 Rebalance
2. **统一确认机制**：通过 AckMessage 确认消费成功
3. **超时自动重试**：ChangeInvisibleTime 修改可见时间
4. **FIFO 支持**：同 key 消息严格有序

### 处理流程

```
1. processRequest(ctx, request)
   ├── 解析 PopMessageRequestHeader
   ├── 构建 Filter（支持 Tag / SQL92）
   ├── 获取消息
   │   ├── 先从 PopBufferMergeService 获取（内存缓存）
   │   └── 再从 MessageStore 获取
   ├── 构建 PopCheckPoint（检查点）
   │   ├── topic, queueId, offset
   │   ├── invisibleTime（可见时间窗口）
   │   └── startOffset, num（批次信息）
   ├── 注册长轮询（没有新消息时）
   └── 返回消息 + PopCheckPoint
```

### PopCheckPoint

```java
public class PopCheckPoint {
    private String topic;
    private int queueId;
    private long startOffset;     // 起始 offset
    private int num;              // 消息数量
    private long invisibleTime;   // 可见时间窗口
    private long reviveOffset;    // Revive 队列中的 offset
}
```

## 🔧 AdminBrokerProcessor（运维管理）

支持大量管理命令，是运维和调试的核心入口：

| RequestCode | 功能 |
|---|---|
| UPDATE_AND_CREATE_TOPIC | 创建/更新 Topic |
| DELETE_TOPIC | 删除 Topic |
| GET_ALL_TOPIC_CONFIG | 获取所有 Topic 配置 |
| UPDATE_BROKER_CONFIG | 动态更新 Broker 配置 |
| GET_BROKER_CONFIG | 获取 Broker 配置 |
| GET_BROKER_RUNTIME_INFO | 获取 Broker 运行时信息 |
| SEARCH_OFFSET_BY_TIMESTAMP | 按时间戳查询 Offset |
| GET_MAX_OFFSET / GET_MIN_OFFSET | 获取最大/最小 Offset |
| VIEW_MESSAGE_BY_ID | 根据消息 ID 查看消息 |
| QUERY_MESSAGE | 条件查询消息 |
| RESET_CONSUMER_CLIENT_OFFSET | 重置消费进度 |
| GET_CONSUMER_CONNECTION | 获取消费者连接信息 |
| GET_PRODUCER_CONNECTION | 获取生产者连接信息 |

## 🔄 ConsumerManageProcessor（Offset 管理）

```java
// 查询消费进度
case RequestCode.QUERY_CONSUMER_OFFSET:
    return queryConsumerOffset(ctx, request);

// 更新消费进度
case RequestCode.UPDATE_CONSUMER_OFFSET:
    return updateConsumerOffset(ctx, request);

// 查询消费者列表
case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
    return getConsumerListByGroup(ctx, request);
```

## 💡 设计亮点

1. **处理器与线程池解耦**：每个 Processor 可以绑定独立的线程池
2. **Pipeline 集成**：处理器通过 `RequestPipeline` 与 Proxy 模式无缝集成
3. **统一的错误处理**：`AbortProcessException` 统一中断异常流程
4. **Metrics 埋点**：每个处理器都集成 OpenTelemetry 指标采集
5. **优雅关闭**：通过 `GO_AWAY` 响应码通知客户端迁移

## 🔗 与其他模块的关系

- **Remoting 层**：处理器注册到 `processorTable`，由 Netty 分发调用
- **Store 层**：处理器调用 `MessageStore` 读写消息
- **Proxy 层**：Proxy 的 `DefaultMessagingProcessor` 最终委托给 Broker 处理器
- **长轮询**：`PullRequestHoldService` / `PopLongPollingService` 管理挂起的请求

---

#核心类 #设计模式
