# LitePullConsumer 轻量级拉消费

> 源码位置：`client/src/main/java/org/apache/rocketmq/client/consumer/DefaultLitePullConsumer.java` + `impl/consumer/DefaultLitePullConsumerImpl.java`

## 🏗️ 什么是 LitePullConsumer

LitePullConsumer 是 5.x 引入的**轻量级拉消费者**，与传统的 PushConsumer 不同，它提供更底层的控制。

```
PushConsumer（Push 模式）：
├── 长轮询自动拉取
├── 消息自动分发到 MessageListener
├── 自动 Rebalance
└── 适合：简单消费场景

LitePullConsumer（Pull 模式）：
├── 主动拉取消息
├── 手动控制拉取节奏
├── 支持 assign（手动分配）和 subscribe（自动 Rebalance）
└── 适合：流处理、批量处理、自定义消费节奏
```

## 📊 两种消费模式

### 1. subscribe 模式（自动 Rebalance）

```java
DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("ConsumerGroup");
consumer.subscribe("TopicTest", "*");

// 自动拉取并缓存
while (running) {
    List<MessageExt> msgs = consumer.poll(1000);  // 阻塞拉取
    for (MessageExt msg : msgs) {
        // 处理消息
    }
    consumer.commitSync();  // 手动提交 offset
}
```

### 2. assign 模式（手动分配）

```java
DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("ConsumerGroup");

// 手动分配队列
consumer.assign(Arrays.asList(
    new MessageQueue("TopicTest", "Broker-1", 0),
    new MessageQueue("TopicTest", "Broker-1", 1)
));

// 从指定 offset 开始消费
consumer.seek(new MessageQueue("TopicTest", "Broker-1", 0), 1000);

while (running) {
    List<MessageExt> msgs = consumer.poll(1000);
    for (MessageExt msg : msgs) {
        // 处理消息
    }
    consumer.commitSync();
}
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `DefaultLitePullConsumer` | 对外接口 |
| `DefaultLitePullConsumerImpl` | 内部实现 |
| `LitePullConsumer` | 消费者接口 |
| `RebalanceLitePullImpl` | Rebalance 实现 |
| `AssignedMessageQueue` | 手动分配的队列管理 |
| `PullTaskImpl` | 拉取任务（每个队列一个） |

## 🔄 核心流程

### 启动流程

```java
public synchronized void start() throws MQClientException {
    // 1. 初始化
    this.rebalanceImpl = new RebalanceLitePullImpl(...);
    this.pullAPIWrapper = new PullAPIWrapper(...);

    // 2. 启动 MQClientInstance
    mQClientFactory.start();

    // 3. 启动拉取任务
    if (model == MessageModel.CLUSTERING) {
        // subscribe 模式：Rebalance 后自动启动 PullTask
    } else {
        // assign 模式：手动分配后启动 PullTask
    }
}
```

### 拉取任务（PullTaskImpl）

```java
class PullTaskImpl implements Runnable {
    private final MessageQueue messageQueue;
    private volatile boolean cancelled = false;

    @Override
    public void run() {
        while (!cancelled) {
            try {
                // 1. 检查是否暂停
                if (isPaused(messageQueue)) {
                    Thread.sleep(100);
                    continue;
                }

                // 2. 获取当前 offset
                long offset = nextPullOffset(messageQueue);

                // 3. 拉取消息
                PullResult pullResult = pull(messageQueue, offset, batchSize);

                // 4. 处理结果
                switch (pullResult.getStatus()) {
                    case FOUND:
                        // 缓存消息到本地队列
                        putMessage(messageQueue, pullResult.getMsgFoundList());
                        updateOffset(messageQueue, pullResult.getNextBeginOffset());
                        break;
                    case NO_NEW_MSG:
                        // 没有新消息，等待后重试
                        Thread.sleep(pullTimeDelayMillsWhenNoNewMsg);
                        break;
                    case OFFSET_ILLEGAL:
                        // Offset 非法，修正
                        long correctedOffset = pullResult.getNextBeginOffset();
                        updateOffset(messageQueue, correctedOffset);
                        break;
                }
            } catch (Exception e) {
                Thread.sleep(pullTimeDelayMillsWhenException);
            }
        }
    }
}
```

### poll() 方法

```java
public List<MessageExt> poll(long timeout) {
    // 1. 从本地缓存队列取消息
    List<MessageExt> msgs = consumeMessageCache.poll(timeout, TimeUnit.MILLISECONDS);

    // 2. 如果缓存为空，触发拉取
    if (msgs == null || msgs.isEmpty()) {
        return Collections.emptyList();
    }

    return msgs;
}
```

## 🎯 与 PushConsumer 的对比

| 特性 | PushConsumer | LitePullConsumer |
|---|---|---|
| 消息获取 | 自动推送（长轮询） | 主动 poll() |
| 消费节奏 | Broker 控制 | 客户端控制 |
| Rebalance | 自动 | subscribe 模式自动，assign 模式手动 |
| Offset 管理 | 自动提交 | 手动 commitSync() |
| 暂停/恢复 | 不支持 | 支持 pause()/resume() |
| 指定 offset | 不支持 | 支持 seek() |
| 适用场景 | 实时消费 | 流处理、批量处理 |
| 消息缓存 | ProcessQueue | 本地队列 |

## 💡 设计亮点

1. **两种模式**：subscribe（自动）和 assign（手动）灵活选择
2. **poll 模式**：客户端控制消费节奏，避免被压垮
3. **手动提交**：commitSync() 保证消息不丢失
4. **pause/resume**：支持暂停和恢复消费
5. **seek**：支持从任意 offset 开始消费

## ⚠️ 注意事项

1. **手动 commit**：忘记 commitSync 会导致消息重复消费
2. **线程安全**：poll() 和 commitSync() 需要在同一线程
3. **缓存大小**：pullThresholdForQueue 控制本地缓存大小
4. **assign 模式**：需要手动管理队列分配和 Rebalance

## 🔗 与其他模块的关系

- **RebalanceLitePullImpl**：subscribe 模式下的 Rebalance
- **PullAPIWrapper**：封装拉取请求
- **MessageQueue**：assign 模式手动指定队列
- **长轮询笔记**：`20-Push消费与Pull消费` 提到了 LitePull，本文展开实现

---

#核心类 #设计模式
