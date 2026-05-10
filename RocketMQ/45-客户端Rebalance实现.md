# 客户端 Rebalance 实现

> 源码位置：`client/src/main/java/org/apache/rocketmq/client/impl/consumer/Rebalance*.java`

## 🏗️ 什么是 Rebalance

Rebalance（重平衡）是消费者**动态分配队列**的机制——当消费者数量变化时，自动重新分配每个消费者负责的队列。

```
初始状态（3 个队列，2 个消费者）：
Consumer-A → Queue-0, Queue-1
Consumer-B → Queue-2

新 Consumer-C 加入（触发 Rebalance）：
Consumer-A → Queue-0
Consumer-B → Queue-1
Consumer-C → Queue-2

Consumer-B 离开（触发 Rebalance）：
Consumer-A → Queue-0, Queue-1
Consumer-C → Queue-2
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                   Rebalance 体系                         │
├─────────────────────────────────────────────────────────┤
│              RebalanceImpl (抽象基类)                    │
│  ┌──────────┬──────────┬──────────────────────────┐    │
│  │processQ  │topicSub  │allocateMessageQueue      │    │
│  │ueueTable │scribeInfo│Strategy                  │    │
│  │(队列→PQ) │Table     │(分配策略)                 │    │
│  └──────────┴──────────┴──────────────────────────┘    │
├─────────────────────────────────────────────────────────┤
│  ┌──────────────┬──────────────┬──────────────────┐    │
│  │RebalancePush │RebalancePull │RebalanceLitePull │    │
│  │Impl          │Impl          │Impl              │    │
│  │(Push模式)    │(Pull模式)    │(LitePull模式)    │    │
│  └──────────────┴──────────────┴──────────────────┘    │
├─────────────────────────────────────────────────────────┤
│              RebalanceService (定时触发)                  │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `RebalanceImpl` | 抽象基类，核心 Rebalance 逻辑 |
| `RebalancePushImpl` | Push 模式实现 |
| `RebalancePullImpl` | Pull 模式实现 |
| `RebalanceLitePullImpl` | LitePull 模式实现 |
| `RebalanceService` | 定时触发 Rebalance 的服务线程 |
| `ProcessQueue` | 处理队列（消费中的消息） |
| `AllocateMessageQueueStrategy` | 队列分配策略 |

## 🔄 Rebalance 触发时机

```
1. 定时触发（每 20 秒）
   └── RebalanceService.run()

2. 消费者数量变化
   └── 收到 NOTIFY_CONSUMER_IDS_CHANGED 请求

3. Topic 队列数变化
   └── 收到 Topic 路由更新

4. 消费者启动
   └── DefaultMQPushConsumer.start()
```

## 📋 核心流程

### RebalanceImpl.rebalance()

```java
public void rebalance(final String topic) {
    // 1. 获取 Topic 的队列信息
    Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
    if (mqSet == null || mqSet.isEmpty()) {
        return;
    }

    // 2. 获取消费者列表
    List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
    if (cidAll == null || cidAll.isEmpty()) {
        return;
    }

    // 3. 排序（保证所有消费者看到相同的排序结果）
    Collections.sort(mqSet);
    Collections.sort(cidAll);

    // 4. 调用分配策略
    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;
    List<MessageQueue> allocateResult = strategy.allocate(
        this.consumerGroup,
        this.mQClientFactory.getClientId(),
        mqSet,
        cidAll
    );

    // 5. 处理分配结果
    Set<MessageQueue> allocateResultSet = new HashSet<>(allocateResult);
    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet);

    // 6. 如果有变化，更新订阅信息
    if (changed) {
        this.messageQueueChanged(topic, mqSet, allocateResultSet);
    }
}
```

### updateProcessQueueTableInRebalance()

```java
private boolean updateProcessQueueTableInRebalance(String topic, Set<MessageQueue> mqSet) {
    boolean changed = false;

    // 1. 移除不再分配给自己的队列
    Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
    while (it.hasNext()) {
        Entry<MessageQueue, ProcessQueue> entry = it.next();
        MessageQueue mq = entry.getKey();
        if (mq.getTopic().equals(topic) && !mqSet.contains(mq)) {
            // 移除
            it.remove();
            changed = true;
        }
    }

    // 2. 添加新分配给自己的队列
    for (MessageQueue mq : mqSet) {
        if (!this.processQueueTable.containsKey(mq)) {
            // 新队列，创建 ProcessQueue
            ProcessQueue pq = new ProcessQueue();
            long offset = this.computePullFromWhere(mq);
            pq.setPullOffset(offset);

            this.processQueueTable.put(mq, pq);
            changed = true;

            // 3. 触发拉取
            this.dispatchPullRequest(Collections.singletonList(new PullRequest(
                consumerGroup, mq, pq, offset)), 100);
        }
    }

    return changed;
}
```

## 🎯 分配策略

### 1. AllocateMessageQueueAveragely（平均分配）⭐ 默认

```java
// 算法：按序号平均分配
// 6 个队列，3 个消费者
// Consumer-0 → Q0, Q1
// Consumer-1 → Q2, Q3
// Consumer-2 → Q4, Q5

List<MessageQueue> allocate(String consumerGroup, String currentCID,
    List<MessageQueue> mqAll, List<String> cidAll) {
    int index = cidAll.indexOf(currentCID);
    int mod = mqAll.size() % cidAll.size();
    int averageSize = mqAll.size() / cidAll.size();
    int startIndex = index * averageSize + Math.min(index, mod);
    int range = index < mod ? averageSize + 1 : averageSize;

    return mqAll.subList(startIndex, startIndex + range);
}
```

### 2. AllocateMessageQueueAveragelyByCircle（环形平均分配）

```java
// 算法：环形轮流分配
// 6 个队列，3 个消费者
// Consumer-0 → Q0, Q3
// Consumer-1 → Q1, Q4
// Consumer-2 → Q2, Q5

List<MessageQueue> allocate(...) {
    List<MessageQueue> result = new ArrayList<>();
    int index = cidAll.indexOf(currentCID);
    for (int i = 0; i < mqAll.size(); i++) {
        if (i % cidAll.size() == index) {
            result.add(mqAll.get(i));
        }
    }
    return result;
}
```

### 3. AllocateMessageQueueConsistentHash（一致性 Hash）

```java
// 算法：一致性 Hash 环
// 每个消费者虚拟 10 个节点
// 队列按 hash 值分配到最近的消费者节点

// 优势：消费者变化时，只有少量队列需要迁移
```

### 4. AllocateMachineRoomNearby（就近机房优先）

```java
// 算法：优先分配到同机房的 Broker
// 跨机房分配作为兜底
```

## 📊 ProcessQueue（处理队列）

```java
public class ProcessQueue {
    // 消息存储（按 offset 排序）
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<>();

    // 拉取偏移量
    private volatile long pullOffset = 0;

    // 消费偏移量
    private volatile long consumedOffset = 0;

    // 消息数量
    private final AtomicInteger msgCount = new AtomicInteger(0);

    // 是否被丢弃
    private volatile boolean dropped = false;

    // 上次拉取时间
    private volatile long lastPullTimestamp = System.currentTimeMillis();

    // 上次消费时间
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
}
```

## 💡 设计亮点

1. **排序一致性**：所有消费者对队列和消费者列表排序，保证分配结果一致
2. **增量更新**：只处理变化的队列，不全量重建
3. **多策略支持**：平均、环形、一致性 Hash、就近机房
4. **定时触发**：RebalanceService 每 20 秒检查一次
5. **优雅停止**：消费者关闭时主动释放队列

## ⚠️ 注意事项

1. **Rebalance 风暴**：大量消费者同时变化可能导致频繁 Rebalance
2. **消费暂停**：Rebalance 过程中会暂停消费
3. **消息重复**：队列迁移可能导致消息重复消费
4. **分配策略选择**：一致性 Hash 适合消费者频繁变化的场景
5. **顺序消息**：顺序消息必须使用固定队列策略

## 🔗 与其他模块的关系

- **DefaultMQPushConsumer**：Push 模式使用 RebalancePushImpl
- **PullMessageProcessor**：Rebalance 后触发新的拉取请求
- **Broker**：Broker 通知消费者 Rebalance（NOTIFY_CONSUMER_IDS_CHANGED）
- **NameServer**：客户端从 NameServer 获取 Topic 路由

---

#核心类 #设计模式 #面试高频
