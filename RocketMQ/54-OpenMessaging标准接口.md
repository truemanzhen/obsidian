# OpenMessaging 标准接口

> 源码位置：`openmessaging/src/main/java/io/openmessaging/rocketmq/`

## 🏗️ 什么是 OpenMessaging

OpenMessaging 是 Linux 基金会下的**消息标准**项目，定义了统一的消息 API。RocketMQ 提供了 OpenMessaging 标准的实现。

```
┌─────────────────────────────────────────────────────────┐
│                   OpenMessaging 标准                      │
├─────────────────────────────────────────────────────────┤
│  MessagingAccessPoint  → 创建 Producer/Consumer          │
│  Producer              → 发送消息                        │
│  PushConsumer          → 推消费                          │
│  PullConsumer          → 拉消费                          │
│  Message               → 消息抽象                        │
│  BytesMessage          → 字节消息                        │
└─────────────────────────────────────────────────────────┘
         ↓ 实现
┌─────────────────────────────────────────────────────────┐
│                   RocketMQ 实现                          │
├─────────────────────────────────────────────────────────┤
│  MessagingAccessPointImpl → 内部创建 DefaultMQProducer   │
│  ProducerImpl             → 委托 DefaultMQProducer       │
│  PushConsumerImpl         → 委托 DefaultMQPushConsumer   │
│  PullConsumerImpl         → 委托 DefaultMQPullConsumer   │
│  BytesMessageImpl         → 包装 Message                 │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `MessagingAccessPointImpl` | 消息接入点实现（工厂） |
| `ProducerImpl` | 生产者实现 |
| `PushConsumerImpl` | 推消费者实现 |
| `PullConsumerImpl` | 拉消费者实现 |
| `BytesMessageImpl` | 字节消息实现 |
| `OMSUtil` | 工具类 |
| `RocketMQConstants` | RocketMQ 特有常量 |

## 📝 MessagingAccessPointImpl

```java
public class MessagingAccessPointImpl implements MessagingAccessPoint {
    private final Properties properties;
    private final DefaultMQProducer defaultMQProducer;

    public MessagingAccessPointImpl(String endpoint, Properties properties) {
        this.properties = properties;
        // 创建 RocketMQ Producer
        this.defaultMQProducer = new DefaultMQProducer();
        this.defaultMQProducer.setNamesrvAddr(endpoint);
    }

    @Override
    public Producer createProducer() {
        return new ProducerImpl(this.defaultMQProducer);
    }

    @Override
    public PushConsumer createPushConsumer() {
        return new PushConsumerImpl(this.properties);
    }

    @Override
    public PullConsumer createPullConsumer() {
        return new PullConsumerImpl(this.properties);
    }
}
```

## 📤 ProducerImpl

```java
public class ProducerImpl implements Producer {
    private final DefaultMQProducer producer;

    @Override
    public SendResult send(Message message) {
        // 转换为 RocketMQ Message
        Message rmqMessage = OMSUtil.toMessage(message);

        // 委托给 RocketMQ Producer
        org.apache.rocketmq.client.producer.SendResult rmqResult =
            producer.send(rmqMessage);

        // 转换为 OMS SendResult
        return OMSUtil.toSendResult(rmqResult);
    }

    @Override
    public void sendAsync(Message message, SendCallback callback) {
        // 异步发送
    }

    @Override
    public void sendOneway(Message message) {
        // 单向发送
    }
}
```

## 📥 PushConsumerImpl

```java
public class PushConsumerImpl implements PushConsumer {
    private final DefaultMQPushConsumer consumer;

    @Override
    public void attachQueue(String queueName, MessageListener listener) {
        // 注册消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                // 转换为 OMS Message
                BytesMessage omsMsg = OMSUtil.toBytesMessage(msg);
                listener.onReceived(omsMsg, context);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }
}
```

## 🔄 消息转换

```java
public class OMSUtil {
    // OMS Message → RocketMQ Message
    public static Message toMessage(BytesMessage omsMsg) {
        Message rmqMsg = new Message();
        rmqMsg.setTopic(omsMsg.sysHeaders().getString(MessageHeader.QUEUE_NAME));
        rmqMsg.setBody(omsMsg.getBody());
        rmqMsg.setTags(omsMsg.sysHeaders().getString("TAGS"));
        return rmqMsg;
    }

    // RocketMQ Message → OMS Message
    public static BytesMessage toBytesMessage(MessageExt rmqMsg) {
        BytesMessage omsMsg = new BytesMessageImpl();
        omsMsg.setBody(rmqMsg.getBody());
        omsMsg.sysHeaders().put(MessageHeader.QUEUE_NAME, rmqMsg.getTopic());
        return omsMsg;
    }
}
```

## 💡 设计亮点

1. **标准接口**：遵循 OpenMessaging 规范，可移植
2. **委托模式**：内部委托给 RocketMQ 原生客户端
3. **消息转换**：OMS 和 RocketMQ 消息格式双向转换
4. **工厂模式**：MessagingAccessPointImpl 作为工厂创建 Producer/Consumer
5. **非标准扩展**：`NonStandardKeys` 支持 RocketMQ 特有功能

## ⚠️ 注意事项

1. **功能受限**：OpenMessaging 标准不支持 RocketMQ 所有特性
2. **非主流**：大部分用户直接使用 RocketMQ 原生 API
3. **版本兼容**：OpenMessaging API 可能随版本变化
4. **性能开销**：消息转换有额外开销

## 🔗 与其他模块的关系

- **Client 模块**：OpenMessaging 实现委托给 RocketMQ 原生客户端
- **Message**：消息格式转换
- **Producer/Consumer**：与原生客户端功能对应

---

#源码技巧
