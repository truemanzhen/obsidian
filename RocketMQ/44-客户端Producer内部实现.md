# 客户端 Producer 内部实现

> 源码位置：`client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java` + `impl/producer/`

## 🏗️ 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                   DefaultMQProducer                      │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │              DefaultMQProducerImpl                  │ │
│  │  ┌──────────┬──────────┬──────────────────────┐   │ │
│  │  │MQClient  │TopicRoute│SendMessageHook       │   │ │
│  │  │Instance  │Info      │(Trace/ACL)           │   │ │
│  │  └──────────┴──────────┴──────────────────────┘   │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │           发送方式                                  │ │
│  │  send()        sendAsync()       sendOneway()      │ │
│  │  (同步)        (异步)            (单向)             │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `DefaultMQProducer` | 生产者对外接口 |
| `DefaultMQProducerImpl` | 生产者内部实现 |
| `MQClientInstance` | 客户端实例（管理连接、路由） |
| `TopicPublishInfo` | Topic 发布信息（队列列表、路由） |
| `SendMessageHook` | 发送钩子（Trace、ACL） |
| `ProduceAccumulator` | 消息累积器（批量发送） |

## 📤 发送流程

### 同步发送

```java
public SendResult send(Message msg) throws MQClientException, RemotingException,
    MQBrokerException, InterruptedException {
    return send(msg, this.getSendMsgTimeout());
}

public SendResult send(Message msg, long timeout) throws ... {
    // 1. 校验消息
    Validators.checkMessage(msg, this);

    // 2. 设置 Topic 命名空间
    msg.setTopic(withNamespace(msg.getTopic()));

    // 3. 调用实现
    return this.defaultMQProducerImpl.sendDefaultImpl(msg, CommunicationMode.SYNC,
        null, timeout);
}
```

### sendDefaultImpl 核心流程

```java
private SendResult sendDefaultImpl(Message msg, CommunicationMode communicationMode,
    SendCallback sendCallback, long timeout) throws ... {

    // 1. 获取 Topic 路由信息
    TopicPublishInfo tpInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
    if (tpInfo == null || !tpInfo.ok()) {
        throw new MQClientException("No route info for topic: " + msg.getTopic());
    }

    // 2. 重试循环
    int timesTotal = communicationMode == CommunicationMode.SYNC
        ? 1 + this.getRetryTimesWhenSendFailed()  // 同步：1 + retryTimes
        : 1;                                       // 异步/单向：不重试

    for (int times = 0; times < timesTotal; times++) {
        // 3. 选择队列
        MessageQueue mqSelected = selectOneMessageQueue(tpInfo, lastBrokerName);
        if (mqSelected == null) {
            break;
        }

        try {
            // 4. 发送消息
            SendResult sendResult = this.sendKernelImpl(msg, mqSelected,
                communicationMode, sendCallback, timeout);

            // 5. 处理结果
            switch (communicationMode) {
                case SYNC:
                    if (sendResult == null) {
                        continue; // 重试
                    }
                    return sendResult;
                case ASYNC:
                    return null; // 回调处理
                case ONEWAY:
                    return null;
            }
        } catch (Exception e) {
            // 6. 失败处理
            if (retryAnotherBrokerWhenNotStoreOK && ...) {
                lastBrokerName = mqSelected.getBrokerName();
                continue; // 换 Broker 重试
            }
            throw e;
        }
    }

    throw new MQClientException("Send failed after retry", lastException);
}
```

## 🎯 队列选择策略

### 默认策略（轮询）

```java
public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo,
    final String lastBrokerName) {
    return tpInfo.selectOneMessageQueue(lastBrokerName);
}

// TopicPublishInfo 内部
public MessageQueue selectOneMessageQueue(String lastBrokerName) {
    // 轮询索引
    int index = this.sendWhichQueue.getAndIncrement();
    int pos = Math.abs(index) % this.messageQueueList.size();

    // 避免上次失败的 Broker
    for (int i = 0; i < this.messageQueueList.size(); i++) {
        int pos = Math.abs(index++) % this.messageQueueList.size();
        MessageQueue mq = this.messageQueueList.get(pos);
        if (!mq.getBrokerName().equals(lastBrokerName)) {
            return mq;
        }
    }
    return this.messageQueueList.get(pos);
}
```

### 自定义选择器

```java
// 通过 MessageQueueSelector 自定义
public interface MessageQueueSelector {
    MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}

// 内置实现
SelectMessageQueueByRandom      // 随机选择
SelectMessageQueueByHash        // 按 hash 选择（顺序消息）
SelectMessageQueueByMachineRoom // 按机房选择
```

## 🔄 重试机制

### 同步重试

```java
// 配置
producer.setRetryTimesWhenSendFailed(2);      // 同步失败重试次数
producer.setRetryAnotherBrokerWhenNotStoreOK(true);  // 换 Broker 重试

// 重试逻辑
for (int times = 0; times < timesTotal; times++) {
    try {
        SendResult result = sendKernelImpl(...);
        if (result != null) {
            return result;
        }
    } catch (RemotingException e) {
        // 网络异常，重试
        lastBrokerName = mqSelected.getBrokerName();
        log.warn("send message to {} failed, retry...", mqSelected);
    } catch (MQBrokerException e) {
        // Broker 异常
        switch (e.getResponseCode()) {
            case ResponseCode.TOPIC_NOT_EXIST:
            case ResponseCode.SERVICE_NOT_AVAILABLE:
                // 可重试
                lastBrokerName = mqSelected.getBrokerName();
                continue;
            default:
                // 不可重试
                throw e;
        }
    }
}
```

### 异步重试

```java
producer.setRetryTimesWhenSendAsyncFailed(2);

// 异步重试在回调中处理
sendCallback = new SendCallback() {
    @Override
    public void onSuccess(SendResult sendResult) { ... }

    @Override
    public void onException(Throwable e) {
        if (retryTimes < maxRetryTimes) {
            // 重新发送
            sendKernelImpl(msg, mq, CommunicationMode.ASYNC, this, timeout);
        }
    }
};
```

## ⏱️ 超时控制

```java
// 发送超时
producer.setSendMsgTimeout(3000);  // 3 秒

// 超时处理
private SendResult sendKernelImpl(Message msg, MessageQueue mq,
    CommunicationMode communicationMode, SendCallback sendCallback, long timeout) {

    // 查找 Broker 地址
    String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());

    // 设置唯一 ID
    if (!msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).isEmpty()) {
        msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            UtilAll.getIP() + "#" + msgId);
    }

    // 发送
    switch (communicationMode) {
        case SYNC:
            return this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                brokerAddr, mq.getBrokerName(), msg, requestHeader, timeout, ...);
        case ASYNC:
            this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                brokerAddr, mq.getBrokerName(), msg, requestHeader, timeout, sendCallback, ...);
            return null;
        case ONEWAY:
            this.mQClientFactory.getMQClientAPIImpl().sendMessageOneway(
                brokerAddr, mq.getBrokerName(), msg, requestHeader, timeout);
            return null;
    }
}
```

## 💡 设计亮点

1. **透明重试**：同步模式自动重试，业务无感知
2. **队列轮询**：均匀分布消息到各队列
3. **Broker 回避**：失败的 Broker 短时间内不再选择
4. **Hook 机制**：Trace、ACL 通过 Hook 无侵入集成
5. **三种通信模式**：同步/异步/单向满足不同场景

## ⚠️ 注意事项

1. **重复消息**：重试可能导致消息重复（需要幂等）
2. **超时设置**：太短会误判，太长会阻塞
3. **队列选择**：顺序消息需要固定队列
4. **sendWhichQueue**：ThreadLocal 变量，注意线程安全

## 🔗 与其他模块的关系

- **MQClientInstance**：管理连接、路由、心跳
- **TopicPublishInfo**：缓存 Topic 路由信息
- **SendMessageHook**：Trace 和 ACL 的集成点
- **Remoting 层**：最终通过 Netty 发送

---

#核心类 #面试高频
