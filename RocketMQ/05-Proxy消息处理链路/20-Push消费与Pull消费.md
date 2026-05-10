# Push 消费与 Pull 消费 — 两种经典消费模型

> 📄 源码路径：`broker/src/main/java/org/apache/rocketmq/broker/processor/`
> 🏷️ #核心类 #面试高频

## 🎯 三种消费模式全景

```
┌─────────────────────────────────────────────────────────────────────┐
│                    RocketMQ 5.x 三种消费模式                         │
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │   Pull 消费      │  │   Push 消费      │  │   Pop 消费       │     │
│  │  (消费者主动拉)   │  │  (长轮询模拟推)   │  │  (服务端调度)     │     │
│  │                 │  │                 │  │                 │     │
│  │ Client → Pull() │  │ Client → Pull() │  │ Client → Pop()  │     │
│  │ 短轮询          │  │ + Long Polling  │  │ Broker 端调度    │     │
│  │ Consumer 感知 Q │  │ Consumer 感知 Q │  │ Consumer 无感知 Q│     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                     │
│  4.x 经典        4.x 主流          5.x 新特性                       │
│  低级 API        高级 API          Proxy 模式                       │
└─────────────────────────────────────────────────────────────────────┘
```

## 📐 Pull 消费 — 消费者主动拉取

### 核心流程

```
Consumer                    Broker (PullMessageProcessor)
  │                              │
  │ PullMessageRequest           │
  │ (topic, queueId, offset)     │
  │─────────────────────────────→│
  │                              │ ① 权限校验
  │                              │ ② 订阅校验
  │                              │ ③ messageStore.getMessage()
  │                              │    → ConsumeQueue → CommitLog
  │                              │
  │  ┌───────────────────────┐   │
  │  │ 有消息？               │   │
  │  │                       │   │
  │  │ YES → 返回消息         │   │
  │  │ NO  → 长轮询 or 超时   │   │
  │  └───────────────────────┘   │
  │                              │
  │ PullMessageResponse          │
  │ (messages, nextBeginOffset)  │
  │←─────────────────────────────│
```

### PullMessageProcessor.processRequest() 关键代码

```java
// ① 权限校验
if (!PermName.isReadable(brokerPermission)) {
    response.setCode(ResponseCode.NO_PERMISSION);
    return response;
}

// ② 订阅校验
SubscriptionGroupConfig subscriptionGroupConfig =
    brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupId);
if (subscriptionGroupConfig == null) {
    response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
    return response;
}

// ③ 消息过滤
MessageFilter messageFilter = new ExpressionMessageFilter(
    subscriptionData, consumerFilterData, consumerFilterManager);

// ④ 从存储层取消息
messageStore.getMessageAsync(group, topic, queueId, queueOffset,
    maxMsgNums, messageFilter)
    .thenApply(result -> pullMessageResultHandler.handle(result, ...));
```

### Pull 长轮询 — PullRequestHoldService

```java
public class PullRequestHoldService extends ServiceThread {
    // 挂起的 Pull 请求表：topic@queueId → 请求列表
    protected ConcurrentMap<String, ManyPullRequest> pullRequestTable;

    // 挂起请求（没有消息时）
    public void suspendPullRequest(String topic, int queueId, PullRequest pullRequest) {
        String key = buildKey(topic, queueId);
        ManyPullRequest mpr = pullRequestTable.get(key);
        mpr.addPullRequest(pullRequest);
    }

    // 定时检查 + 消息到达通知
    protected void checkHoldRequest() {
        for (String key : pullRequestTable.keySet()) {
            String topic = kArray[0];
            int queueId = Integer.parseInt(kArray[1]);
            long offset = messageStore.getMaxOffsetInQueue(topic, queueId);
            notifyMessageArriving(topic, queueId, offset);  // 唤醒等待的请求
        }
    }

    // 消息到达时唤醒
    public void notifyMessageArriving(String topic, int queueId, long maxOffset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

        List<PullRequest> requestList = mpr.cloneListAndClear();
        for (PullRequest request : requestList) {
            if (newestOffset > request.getPullFromThisOffset()) {
                // 有新消息！重新执行 Pull 请求
                pullMessageProcessor.executeRequestWhenWakeup(request.getClientChannel(),
                    request.getRequestCommand());
            } else if (System.currentTimeMillis() >= request.getSuspendTimestamp() + request.getTimeoutMillis()) {
                // 超时！返回空结果
                pullMessageProcessor.executeRequestWhenWakeup(...);
            } else {
                // 继续等待
                replayList.add(request);
            }
        }
    }
}
```

> [!important] Pull 长轮询的工作原理
> ```
> Consumer 发来 Pull 请求，Queue 中没有新消息：
>
> ① Broker 不立即返回"没有消息"
> ② 将请求挂起到 pullRequestTable
> ③ PullRequestHoldService 定时检查（每 5 秒）
>    或 ReputMessageService 分发时调用 notifyMessageArriving()
> ④ 有新消息到达 → 唤醒挂起的请求 → 重新执行 Pull
> ⑤ 超时 → 返回空结果 → Consumer 重新发起 Pull
> ```
>
> **本质：用"拉"的方式模拟"推"的效果。**

---

## 📐 Push 消费 — 长轮询模拟推

### Push vs Pull 的本质区别

```
Pull 消费（低级 API）：
  Consumer 自己管理：
  - 消费进度（offset）
  - Queue 分配（Rebalance）
  - 重试逻辑
  - 拉取时机

Push 消费（高级 API）：
  Client SDK 内部管理：
  - 自动 Rebalance → 分配 Queue
  - 自动 Pull + Long Polling → 模拟"推"
  - 自动提交 offset
  - 自动重试
  用户只需要实现 MessageListener 接口
```

> [!tip] Push 消费不是真的"推"
> RocketMQ **没有** Broker 主动推消息给 Consumer 的机制。
> Push 消费的底层仍然是 **Pull + Long Polling**。
>
> ```java
> // DefaultMQPushConsumer 内部
> pullMessageService.executePullRequestImmediately(new PullRequest(...));
> // → 调用 PullMessageProcessor.processRequest()
> // → 没有消息时挂起（长轮询）
> // → 有消息时唤醒，返回消息
> // → 处理完后立即发起下一次 Pull
> ```

### Push 消费的消费端流程

```
DefaultMQPushConsumer
    │
    ├─ RebalanceService（定期 Rebalance）
    │     → 分配 Queue 给当前 Consumer
    │     → 生成 PullRequest
    │
    ├─ PullMessageService（拉取线程）
    │     → 从 PullRequest 队列取出请求
    │     → 调用 Broker 的 PullMessageProcessor
    │     → 没有消息时阻塞（长轮询）
    │     → 有消息时回调 MessageListener
    │
    ├─ ConsumeMessageService（消费线程池）
    │     → 执行用户的 MessageListener.consumeMessage()
    │     → 提交消费进度
    │
    └─ OffsetStore（消费进度管理）
          → 本地文件 或 远程 Broker 存储
```

---

## 📐 Pop 消费 — 服务端调度（5.x 新特性）

### Pop vs Push 的本质区别

```
Push 消费（4.x）：
  Consumer 负责：
  - Rebalance（分配 Queue）  ← 最复杂的部分
  - Pull（拉取消息）
  - 管理消费进度
  - 处理 Queue 锁
  Consumer 重，有状态

Pop 消费（5.x）：
  Consumer 只负责：
  - Pop（弹出消息）
  - ACK / 续期
  Broker 负责：
  - 调度（决定从哪个 Queue 取）
  - 管理消费进度（PopConsumerRecord）
  - 超时重试（Revive）
  Consumer 轻，无状态
```

### Pop 消费端流程

```
DefaultMQPopConsumer
    │
    ├─ popMessage(topic, batchSize, invisibleTime)
    │     → 调用 Broker 的 PopMessageProcessor
    │     → Broker 端：从多个 Queue 合并消息
    │     → 返回消息 + PopCheckPoint
    │
    ├─ 处理消息
    │     → 用户消费逻辑
    │
    ├─ ackMessage(offset)
    │     → 调用 Broker 的 AckMessageProcessor
    │     → Broker 删除 PopConsumerRecord
    │
    └─ changeInvisibleDuration(newTime)
          → 调用 Broker 的 ChangeInvisibleTimeProcessor
          → Broker 更新 PopConsumerRecord
```

---

## 📊 三种消费模式对比

| 维度 | Pull | Push | Pop |
|------|------|------|-----|
| **拉取方式** | Consumer 主动拉 | Pull + Long Polling | Consumer Pop |
| **Queue 感知** | 感知（指定 queueId） | 感知（Rebalance 分配） | 不感知（Broker 调度） |
| **消费进度** | Consumer 管理 | Consumer 管理 | Broker 管理 |
| **长轮询** | 支持 | 支持 | 支持 |
| **顺序消费** | Consumer 保证 | Queue 级别锁定 | ConsumerOrderInfoManager |
| **重试** | Consumer 自行处理 | Broker 端 Retry Topic | Revive 服务自动重试 |
| **客户端复杂度** | 高 | 中 | 低 |
| **服务端复杂度** | 低 | 低 | 高 |
| **适用场景** | 自定义消费逻辑 | 通用场景 | Proxy 模式、轻量客户端 |
| **API 级别** | 低级 API | 高级 API | 5.x 新 API |

---

## 📐 Broker 端处理器一览

| Processor | 请求码 | 职责 |
|-----------|--------|------|
| `PullMessageProcessor` | `PULL_MESSAGE` / `LITE_PULL_MESSAGE` | Pull 消费 |
| `PopMessageProcessor` | `POP_MESSAGE` | Pop 消费（5.x） |
| `PopLiteMessageProcessor` | `POP_LITE_MESSAGE` | Lite Pop 消费（5.x） |
| `AckMessageProcessor` | `ACK_MESSAGE` / `BATCH_ACK_MESSAGE` | Pop ACK |
| `ChangeInvisibleTimeProcessor` | `CHANGE_INVISIBLE_TIME` | Pop 续期 |
| `NotificationProcessor` | `NOTIFICATION` | Lite Push 通知 |

## 📐 长轮询实现对比

```
Pull 长轮询（PullRequestHoldService）：
  ┌──────────────────────────────────────────────┐
  │ pullRequestTable:                            │
  │   topicA@0 → [PullReq1, PullReq2, ...]      │
  │   topicA@1 → [PullReq3, ...]                │
  │                                              │
  │ 触发方式：                                    │
  │   ① 定时检查（每 5 秒）                       │
  │   ② ReputMessageService 分发时通知            │
  │   ③ Consumer ACK 时通知（顺序消费）            │
  └──────────────────────────────────────────────┘

Pop 长轮询（PopLongPollingService）：
  ┌──────────────────────────────────────────────┐
  │ pollingMap:                                  │
  │   topic@group → [PopReq1, PopReq2, ...]     │
  │                                              │
  │ 触发方式：                                    │
  │   ① ReputMessageService 分发时通知            │
  │   ② Consumer ACK 时通知                      │
  │   ③ 定时超时检查                              │
  └──────────────────────────────────────────────┘
```

> [!note] 长轮询的核心思想
> 不管是 Pull 还是 Pop，长轮询的核心思想都是一样的：
> 1. 没有消息时，**挂起请求**（不立即返回空）
> 2. 有新消息到达时，**唤醒请求**（重新处理）
> 3. 超时后，**返回空**（Consumer 重新发起请求）
>
> 这样既避免了忙轮询的资源浪费，又保证了消息的实时性。

---

## 💡 面试高频问题

### Q: Push 消费和 Pull 消费有什么区别？

**A：本质都是 Pull。**
- Pull 消费：Consumer 主动调用 `pull()`，自己管理 offset 和 Queue
- Push 消费：底层仍是 Pull + Long Polling，但 SDK 封装了 Rebalance、offset 管理等细节
- Pop 消费：5.x 新特性，Consumer 只管 Pop/ACK，Broker 负责调度和重试

### Q: 长轮询是怎么实现的？

**A：挂起请求 + 消息到达通知。**
1. Consumer 发来 Pull 请求，没有消息 → 请求挂起
2. ReputMessageService 分发新消息到 ConsumeQueue → 调用 `notifyMessageArriving()`
3. 检查挂起的请求：有新消息 → 唤醒，超时 → 返回空
4. 唤醒后重新执行 Pull → 返回消息给 Consumer

### Q: 为什么 5.x 要引入 Pop 消费？

**A：简化客户端，解决 Rebalance 问题。**
- Push 消费的 Rebalance 是最复杂的部分（Queue 分配、Consumer 上下线、Queue 锁）
- Pop 消费把调度逻辑移到 Broker 端，Consumer 变成无状态的
- 好处：扩缩容不需要 Rebalance、客户端更轻量、支持多语言

---

## 💡 学习收获

1. **Push = Pull + Long Polling**：没有真正的"推"，都是拉
2. **长轮询**：挂起请求 + 消息到达通知，兼顾实时性和资源效率
3. **Pop 消费**：把调度逻辑从 Consumer 移到 Broker，简化客户端
4. **三种模式递进**：Pull（手动）→ Push（自动）→ Pop（服务端调度）
5. **处理器模式**：每个消费模式对应一个 Broker 端 Processor
