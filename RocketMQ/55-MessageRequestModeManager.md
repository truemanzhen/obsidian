# MessageRequestModeManager 消息请求模式

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/loadbalance/MessageRequestModeManager.java`

## 🏗️ 什么是消息请求模式

5.x 引入了**消息请求模式管理**，允许 Broker 和客户端协商消息的消费方式。

```
传统模式：
├── 每个消费者通过 Rebalance 分配队列
├── 拉取模式固定
└── 无法动态切换消费模式

5.x 消息请求模式：
├── Pull 模式（默认拉取）
├── Pop 模式（5.x 新增，服务端驱动）
├── LitePull 模式（轻量级拉取）
└── 支持按 Topic/消费组 动态配置
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│              MessageRequestModeManager                    │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │ messageRequestModeMap                              │ │
│  │ Topic@Group → MessageRequestMode                   │ │
│  │                                                    │ │
│  │ 例如:                                              │ │
│  │ TopicA@Group1 → PULL                               │ │
│  │ TopicA@Group2 → POP                                │ │
│  │ TopicB@Group1 → LITE_PULL                          │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `MessageRequestModeManager` | 消息请求模式管理器 |
| `MessageRequestMode` | 消息请求模式枚举 |

## 📝 MessageRequestMode

```java
public class MessageRequestMode {
    private String mode;      // PULL / POP / LITE_PULL
    private Properties properties;  // 模式相关配置

    // 模式枚举
    public static final String PULL = "PULL";
    public static final String POP = "POP";
    public static final String LITE_PULL = "LITE_PULL";
}
```

## 🔄 管理流程

### 设置请求模式

```java
// Admin 设置消费模式
public void updateMessageRequestMode(String topic, String consumerGroup,
    MessageRequestMode mode) {
    String key = topic + "@" + consumerGroup;
    messageRequestModeMap.put(key, mode);

    // 持久化
    persist();

    // 通知消费者
    notifyConsumers(topic, consumerGroup, mode);
}
```

### 查询请求模式

```java
// 消费者启动时查询
public MessageRequestMode getMessageRequestMode(String topic, String consumerGroup) {
    String key = topic + "@" + consumerGroup;
    return messageRequestModeMap.get(key);
}
```

## 🔄 与消费模式的关系

```
MessageRequestModeManager 决定消费模式
    ↓
┌──────────┬──────────────┬──────────────────┐
│  PULL    │  POP         │  LITE_PULL       │
├──────────┼──────────────┼──────────────────┤
│ 传统拉取 │ Pop 消费模式  │ 轻量级拉取       │
│ PullMsg  │ PopMsg       │ LitePull         │
│ Processor│ Processor    │ Consumer         │
│ 长轮询   │ 服务端驱动   │ 客户端控制       │
└──────────┴──────────────┴──────────────────┘
```

## 💡 设计亮点

1. **按 Topic+Group 配置**：不同消费组可以使用不同消费模式
2. **动态切换**：不需要重启即可切换消费模式
3. **统一管理**：集中管理所有 Topic 的消费模式
4. **持久化**：模式配置持久化到文件

## ⚠️ 注意事项

1. **模式兼容**：不同模式的 Offset 管理方式不同，切换时需要注意
2. **消费者重启**：模式切换后消费者需要重启才能生效
3. **Admin 工具**：需要通过 Admin 命令设置消费模式

## 🔗 与其他模块的关系

- **Pop 消费**：POP 模式使用 PopMessageProcessor
- **Pull 消费**：PULL 模式使用 PullMessageProcessor
- **LitePullConsumer**：LITE_PULL 模式使用 DefaultLitePullConsumerImpl
- **Rebalance**：不同模式的 Rebalance 策略不同

---

#源码技巧
