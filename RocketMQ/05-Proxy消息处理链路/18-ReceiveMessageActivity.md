# ReceiveMessageActivity — 消费者 Pop 消费链路

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ReceiveMessageActivity.java`
> 🏷️ #核心类 #面试高频

## 🎯 核心职责

处理消费者的 `receiveMessage` 请求，实现 **Pop 消费模式**（服务端调度）。
这是 5.x 最重要的新特性之一。

## 📐 消费流程

```
ReceiveMessageRequest (gRPC, 长轮询)
    │
    ▼
receiveMessage(ctx, request, responseObserver)
    │
    ├─ ① 获取客户端配置
    │     settings = grpcClientSettingsManager.getClientSettings(ctx)
    │     fifo = subscription.getFifo()
    │     maxAttempts = backoffPolicy.getMaxAttempts()
    │
    ├─ ② 计算长轮询时间
    │     pollingTime = request.getLongPollingTimeout()
    │     限制在 [minPollingTime, maxPollingTime] 范围内
    │     检查 deadline 是否足够
    │
    ├─ ③ 参数校验
    │     validateTopicAndConsumerGroup(topic, group)
    │     validateInvisibleTime(invisibleTime)
    │     buildFilterExpression(filterExpression)
    │
    ├─ ④ 调用 MessagingProcessor
    │     ├─ Lite 模式: messagingProcessor.popLiteMessage(...)
    │     └─ 普通模式: messagingProcessor.popMessage(...)
    │
    ├─ ⑤ 异步处理结果
    │     popFuture.thenAccept(popResult → {
    │         处理自动续期 (autoRenew)
    │         writer.writeAndComplete(ctx, request, popResult)
    │     })
    │
    └─ ⑥ 异常处理
          popFuture.exceptionally(t → {
              writer.writeAndComplete(ctx, request, t)
          })
```

## 🔍 关键步骤详解

### 1. 长轮询时间计算

```java
// 计算轮询时间
long pollingTime;
if (request.hasLongPollingTimeout()) {
    pollingTime = Durations.toMillis(request.getLongPollingTimeout());
} else {
    // 用 deadline 的一半作为轮询时间
    pollingTime = timeRemaining - Durations.toMillis(settings.getRequestTimeout()) / 2;
}

// 限制范围
if (pollingTime < config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
    pollingTime = config.getGrpcClientConsumerMinLongPollingTimeoutMillis();
}
if (pollingTime > config.getGrpcClientConsumerMaxLongPollingTimeoutMillis()) {
    pollingTime = config.getGrpcClientConsumerMaxLongPollingTimeoutMillis();
}

// 检查 deadline
if (pollingTime > timeRemaining) {
    if (timeRemaining >= config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
        pollingTime = timeRemaining;  // 用剩余时间
    } else {
        // 时间不够，返回错误
        writer.writeAndComplete(ctx, Code.ILLEGAL_POLLING_TIME, ...);
        return;
    }
}
```

> [!important] 长轮询的精髓
> Consumer 发来请求后，Proxy 不立即返回"没有消息"，
> 而是**等待一段时间**（长轮询），在等待期间如果有新消息就立即返回。
> 这样既避免了忙轮询的资源浪费，又保证了消息的实时性。

### 2. Pop 消费 vs Push 消费

```
4.x Push 消费（客户端调度）：
  Consumer ←──pull()──→ Broker
  Consumer 内部：Rebalance → 分配 Queue → 拉取消息
  客户端重，需要维护 Queue 绑定关系

5.x Pop 消费（服务端调度）：
  Consumer ←──pop()──→ Proxy ←──pop()──→ Broker
  Proxy/Broker 内部：分配消息给 Consumer
  客户端轻，无状态
```

### 3. Lite 消费者 vs 普通消费者

```java
if (isLite) {
    // Lite 消费者：轻量级，限制未确认消息数
    int unackedMessageCount = messagingProcessor.getUnackedMessageCount(ctx, clientChannel, group);
    if (proxyConfig.getMaxLiteRenewNumPerChannel() < unackedMessageCount) {
        writer.writeAndComplete(ctx, Code.FORBIDDEN, "too many unacked messages");
        return;
    }

    popFuture = this.messagingProcessor.popLiteMessage(...);
} else {
    // 普通消费者
    popFuture = this.messagingProcessor.popMessage(...);
}
```

> [!tip] Lite 消费者
> Lite Push Consumer 是 5.x 新增的轻量消费者类型：
> - 限制未确认消息数量，防止客户端堆积
> - 适合 IoT、移动端等资源受限场景
> - 服务端管理更多消费状态

### 4. 自动续期 (AutoRenew)

```java
final boolean autoRenew = proxyConfig.isEnableProxyAutoRenew() && request.getAutoRenew();

popFuture.thenAccept(popResult -> {
    Runnable doAfterWrite = null;
    if (autoRenew) {
        doAfterWrite = handleAutoRenew(ctx, request, group, topic, popResult, writer);
    }
    writer.writeAndComplete(ctx, request, popResult, doAfterWrite);
});
```

```java
private Runnable handleAutoRenew(...) {
    // 为每条消息注册 ReceiptHandle
    for (MessageExt messageExt : messageExtList) {
        String receiptHandle = messageExt.getProperty(MessageConst.PROPERTY_POP_CK);
        if (receiptHandle != null) {
            MessageReceiptHandle messageReceiptHandle = new MessageReceiptHandle(
                group, topic, queueId, receiptHandle, msgId, queueOffset, reconsumeTimes);
            messagingProcessor.addReceiptHandle(ctx, clientChannel, group, msgId, messageReceiptHandle);
        }
    }
}
```

> [!important] 自动续期机制
> Pop 消费有**可见性超时**（invisibleDuration）：
> - 消费者收到消息后，消息在 Broker 端变为"不可见"
> - 消费者必须在超时前 `ack`，否则消息会被重新投递
> - `autoRenew` 自动延长可见时间，防止慢消费者被重复投递

### 5. Queue 选择 — ReceiveMessageQueueSelector

```java
protected static class ReceiveMessageQueueSelector implements QueueSelector {

    @Override
    public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
        // 优先使用请求中指定的 Broker
        if (StringUtils.isNotBlank(brokerName)) {
            addressableMessageQueue = messageQueueSelector.getQueueByBrokerName(brokerName);
        }

        // 否则随机选择一个可读的 Queue
        if (addressableMessageQueue == null) {
            addressableMessageQueue = messageQueueSelector.selectOne(true);
        }
        return addressableMessageQueue;
    }
}
```

> [!note] 与发送的区别
> - **发送**：按 shardingKey 一致性 Hash 选择 Queue
> - **消费**：按 Broker 名称或随机选择 Queue
> - Pop 模式下，Queue 选择由服务端控制，客户端不感知

## 📊 消费链路完整时序

```
Consumer    GrpcApp    Pipeline    Thread    RecvActivity    MsgProcessor    Broker
  │           │          │          │           │               │              │
  │ Receive   │          │          │           │               │              │
  │──────────→│          │          │           │               │              │
  │           │ Auth     │          │           │               │              │
  │           │──────────│          │           │               │              │
  │           │ submit   │          │           │               │              │
  │           │─────────────────────│           │               │              │
  │           │          │          │ receive   │               │              │
  │           │          │          │──────────→│               │              │
  │           │          │          │           │ 校验参数      │              │
  │           │          │          │           │ buildFilter   │              │
  │           │          │          │           │ selectQueue   │              │
  │           │          │          │           │──────────────→│              │
  │           │          │          │           │               │ popMessage   │
  │           │          │          │           │               │─────────────→│
  │           │          │          │           │               │              │
  │           │          │          │           │               │  长轮询等待  │
  │           │          │          │           │               │    ...       │
  │           │          │          │           │               │  有消息!     │
  │           │          │          │           │               │←─────────────│
  │           │          │          │           │ PopResult     │              │
  │           │          │          │           │←──────────────│              │
  │           │          │          │           │               │              │
  │           │          │          │           │ 转换响应      │              │
  │           │          │          │           │ autoRenew注册 │              │
  │           │          │          │ Result    │               │              │
  │           │          │          │←──────────│               │              │
  │           │          │ Result   │           │               │              │
  │           │←────────────────────│           │               │              │
  │ Result    │          │          │           │               │              │
  │←──────────│          │          │           │               │              │
```

## 💡 设计亮点

### 1. 长轮询 + 超时保护

```
pollingTime 计算逻辑：
  客户端请求的 timeout
    → 减去一半的 requestTimeout（留缓冲）
    → 限制在 [min, max] 范围
    → 检查 deadline 是否足够

好处：
  - 不会因网络延迟导致轮询时间不足
  - 不会因轮询时间过长占用线程
  - deadline 不够时快速失败，不浪费资源
```

### 2. 三级消费模式

| 模式 | 接口 | 特点 |
|------|------|------|
| 普通 Pop | `popMessage` | 通用消费，支持 FIFO |
| Lite Pop | `popLiteMessage` | 轻量消费，限制未确认数 |
| 自动续期 | `autoRenew` | 自动延长可见时间 |

### 3. 防御性编程

- 未确认消息数检查（防止客户端堆积）
- 可见性时间校验（防止过短导致重复投递）
- 过滤表达式校验（防止非法过滤）
- 客户端连接检查（防止向断连客户端发消息）

---

## 🧩 Pop 消费的核心概念

| 概念 | 含义 |
|------|------|
| `invisibleDuration` | 消息不可见时间，超时后消息可被重新投递 |
| `pollingTimeout` | 长轮询等待时间 |
| `receiptHandle` | 消息收据，用于 ack/changeInvisibleDuration |
| `attemptId` | 消费尝试 ID，用于幂等 |
| `autoRenew` | 自动续期，延长消息不可见时间 |
| `maxAttempts` | 最大消费尝试次数 |
| `fifo` | 是否严格顺序消费 |

---

## 💡 学习收获

1. **长轮询设计**：用 deadline 计算轮询时间，平衡实时性和资源消耗
2. **Pop 消费模型**：服务端调度替代客户端 Rebalance
3. **自动续期**：防止慢消费者被重复投递
4. **Lite 消费者**：轻量级消费，适合资源受限场景
5. **全链路异步**：`CompletableFuture` 贯穿始终
