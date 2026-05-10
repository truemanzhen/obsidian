# EscapeBridge Broker 容错桥接

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/failover/EscapeBridge.java`

## 🏗️ 什么是 EscapeBridge

EscapeBridge 是 Broker 的**容错桥接**机制——当 Master Broker 不可用时，自动将消息转发到其他可用的 Broker。

```
正常模式：
Producer → Master Broker → CommitLog
                ↓
           Slave Broker (同步)

Master 不可用时（enableSlaveActingMaster=true）：
Producer → Slave Broker → EscapeBridge
                           ├── 本地存储（如果可写）
                           └── 转发到其他 Master Broker
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                   EscapeBridge                            │
├─────────────────────────────────────────────────────────┤
│  putMessage(MessageExtBrokerInner)                       │
│  ├── 1. 本地 Master 可用 → 直接写入                      │
│  ├── 2. 本地 Master 不可用                               │
│  │   ├── enableSlaveActingMaster=true                    │
│  │   │   ├── enableRemoteEscape=true → 转发到远程 Broker │
│  │   │   └── enableRemoteEscape=false → 写入本地 Slave   │
│  │   └── 返回 SERVICE_NOT_AVAILABLE                      │
│  └── 3. 异步发送到远程 Broker                            │
├─────────────────────────────────────────────────────────┤
│  getMessage() (消费端)                                   │
│  ├── 本地有数据 → 返回本地数据                           │
│  ├── 本地无数据 + SlaveActingMaster                      │
│  │   └── 从远程 Broker 拉取                              │
│  └── 返回 NO_MESSAGE                                    │
└─────────────────────────────────────────────────────────┘
```

## 📤 发送容错

### putMessage 流程

```java
public PutMessageResult putMessage(MessageExtBrokerInner messageExt) {
    // 1. 尝试本地 Master
    BrokerController masterBroker = this.brokerController.peekMasterBroker();
    if (masterBroker != null) {
        return masterBroker.getMessageStore().putMessage(messageExt);
    }

    // 2. Master 不可用，检查是否启用 Slave 接管
    if (brokerController.getBrokerConfig().isEnableSlaveActingMaster()) {
        // 3. 检查是否启用远程 Escape
        if (brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            // 异步发送到远程 Broker
            return putMessageToRemoteBroker(messageExt);
        } else {
            // 写入本地 Slave
            return brokerController.getMessageStore().putMessage(messageExt);
        }
    }

    // 4. 不可用
    return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
}
```

### 异步转发到远程

```java
private PutMessageResult putMessageToRemoteBroker(MessageExtBrokerInner msg) {
    // 1. 构建发送请求
    CompletableFuture<PutMessageResult> future = new CompletableFuture<>();

    // 2. 异步发送
    defaultAsyncSenderExecutor.submit(() -> {
        try {
            // 查找其他可用的 Broker
            SendResult sendResult = sendToRemoteBroker(msg);
            if (sendResult != null && sendResult.getSendStatus() == SendStatus.SEND_OK) {
                future.complete(new PutMessageResult(PutMessageStatus.PUT_OK, null));
            } else {
                future.complete(new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, null));
            }
        } catch (Exception e) {
            future.complete(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }
    });

    // 3. 等待结果
    return future.get(SEND_TIMEOUT, TimeUnit.MILLISECONDS);
}
```

## 📥 消费容错

### getMessage 流程

```java
public GetMessageResult getMessage(String group, String topic, int queueId,
    long offset, int maxMsgNums) {

    // 1. 尝试本地读取
    MessageStore messageStore = brokerController.getMessageStore();
    GetMessageResult result = messageStore.getMessage(group, topic, queueId, offset, maxMsgNums);

    if (result != null && result.getStatus() == GetMessageStatus.FOUND) {
        return result;
    }

    // 2. 本地无数据 + Slave 接管模式
    if (brokerController.getBrokerConfig().isEnableSlaveActingMaster()
        && brokerController.getBrokerConfig().isEnableRemoteEscape()) {
        // 从远程 Broker 拉取
        return getMessageFromRemoteBroker(group, topic, queueId, offset, maxMsgNums);
    }

    return result;
}
```

### 从远程拉取

```java
private GetMessageResult getMessageFromRemoteBroker(String group, String topic,
    int queueId, long offset, int maxMsgNums) {

    // 1. 查找远程 Broker 地址
    String remoteBrokerAddr = findRemoteBrokerAddr(topic, queueId);
    if (remoteBrokerAddr == null) {
        return new GetMessageResult(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
    }

    // 2. 拉取消息
    PullResult pullResult = pullFromRemoteBroker(remoteBrokerAddr, topic, queueId,
        offset, maxMsgNums);

    // 3. 转换结果
    return convertPullResult(pullResult);
}
```

## ⚙️ 配置项

```properties
# 启用 Slave 接管 Master
enableSlaveActingMaster=true

# 启用远程 Escape（转发到其他 Broker）
enableRemoteEscape=true

# 异步发送队列大小
asyncSenderThreadPoolQueueSize=50000

# 发送超时
escapeBridgeSendTimeout=3000
```

## 💡 设计亮点

1. **透明容错**：对 Producer 无感知，自动路由到可用 Broker
2. **异步转发**：远程转发不阻塞本地请求
3. **两种模式**：本地 Slave 写入 + 远程 Broker 转发
4. **消费端也支持**：Slave 接管后可以从远程拉取消息
5. **线程池隔离**：异步转发使用独立线程池

## ⚠️ 注意事项

1. **数据一致性**：远程转发可能丢失消息（异步）
2. **延迟**：远程转发增加延迟
3. **队列满**：异步队列满时会拒绝消息
4. **恢复后同步**：Master 恢复后需要同步 Escape 期间的消息

## 🔗 与其他模块的关系

- **BrokerController**：EscapeBridge 由 BrokerController 管理
- **MessageStore**：本地读写调用 MessageStore
- **HA 服务**：EscapeBridge 是 HA 的补充机制
- **Producer**：Producer 无感知，自动重连到可用 Broker

---

#核心类 #设计模式
