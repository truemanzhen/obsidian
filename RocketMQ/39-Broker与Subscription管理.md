# Broker Topic/Subscription 管理

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/topic/` + `broker/subscription/`

## 🏗️ 整体架构

Topic 和 Subscription（订阅组）是 RocketMQ 的核心元数据，由 Broker 管理并同步到 NameServer。

```
┌─────────────────────────────────────────────────────────┐
│                   Broker 元数据管理                       │
├──────────────────┬──────────────────────────────────────┤
│  TopicConfig     │  SubscriptionGroupConfig             │
│  Manager         │  Manager                             │
│  (Topic 配置)    │  (订阅组配置)                         │
├──────────────────┼──────────────────────────────────────┤
│  topicConfigTable│  subscriptionGroupTable              │
│  (Topic → Config)│  (Group → Config)                    │
├──────────────────┼──────────────────────────────────────┤
│  持久化到文件     │  持久化到文件                         │
│  topic.json      │  subscriptionGroup.json              │
└──────────────────┴──────────────────────────────────────┘
```

## 📋 TopicConfigManager

### 数据结构

```java
public class TopicConfigManager extends ConfigManager {
    // Topic 配置表
    protected ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);

    // 数据版本（用于同步到 NameServer）
    protected DataVersion dataVersion = new DataVersion();
}
```

### TopicConfig

```java
public class TopicConfig {
    private String topicName;           // Topic 名称
    private int readQueueNums = 8;      // 读队列数
    private int writeQueueNums = 8;     // 写队列数
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE; // 权限
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG; // 过滤类型
    private int topicSysFlag = 0;       // 系统标志
    private boolean order = false;      // 是否顺序消息
    private boolean unit = false;       // 是否单元化
    private boolean hasUnitSub = false; // 是否有单元化订阅

    // 5.x 新增属性
    private TopicMessageType topicMessageType;  // 消息类型
    private CleanupPolicy cleanupPolicy;         // 清理策略
}
```

### 核心操作

**1. 创建 Topic**

```java
public void createTopicInSendMessageMethod(final String topic, final String defaultTopic,
    final String remoteAddr, final TopicConfig topicConfig) {

    // 加锁（防止并发创建）
    if (!this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        return;
    }

    try {
        TopicConfig tc = this.topicConfigTable.get(topic);
        if (tc != null) {
            // Topic 已存在，检查是否需要更新
            if (isNeedUpdateTopicConfig(tc, topicConfig)) {
                // 更新配置
                updateTopicConfig(topicConfig);
            }
            return;
        }

        // 新 Topic，创建配置
        topicConfig.setTopicName(topic);
        this.topicConfigTable.put(topic, topicConfig);
        this.dataVersion.nextVersion();

        // 持久化
        this.persist();
    } finally {
        this.topicConfigTableLock.unlock();
    }
}
```

**2. 删除 Topic**

```java
public TopicConfig deleteTopicConfig(final String topic) {
    TopicConfig old = this.topicConfigTable.remove(topic);
    if (old != null) {
        this.dataVersion.nextVersion();
        this.persist();
    }
    return old;
}
```

**3. 查询 Topic**

```java
public TopicConfig selectTopicConfig(final String topic) {
    return this.topicConfigTable.get(topic);
}

public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
    return topicConfigTable;
}
```

### 内置 Topic

```java
// 启动时自动创建的系统 Topic
private void createTopicInBroker() {
    // RMQ_SYS_TRANS_HALF_TOPIC - 事务消息半消息
    // RMQ_SYS_TRANS_OP_HALF_TOPIC - 事务消息操作日志
    // SCHEDULE_TOPIC_XXXX - 延迟消息 Topic
    // TBW102 - 自动创建 Topic 的默认 Topic
    // RMQ_SYS_TRACE_TOPIC - 消息追踪 Topic
    // RMQ_SYS_BENCHMARK_TOPIC - 性能测试 Topic
}
```

## 📋 SubscriptionGroupManager

### 数据结构

```java
public class SubscriptionGroupManager extends ConfigManager {
    // 订阅组配置表
    protected ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
        new ConcurrentHashMap<>(1024);

    // 禁止订阅表（Topic → Group → 禁止原因）
    private ConcurrentMap<String, ConcurrentMap<String, Integer>> forbiddenTable =
        new ConcurrentHashMap<>(4);

    protected DataVersion dataVersion = new DataVersion();
}
```

### SubscriptionGroupConfig

```java
public class SubscriptionGroupConfig {
    private String groupName;              // 消费组名称
    private boolean consumeEnable = true;  // 是否允许消费
    private boolean consumeFromMinEnable = true; // 是否从最小 Offset 消费
    private boolean consumeBroadcastEnable = true; // 是否允许广播消费

    // 重试配置
    private int retryQueueNums = 1;        // 重试队列数
    private int maxReconsumeTimes = 16;    // 最大重试次数（默认 16 次）

    // 消费模式
    private MessageModel messageModel = MessageModel.CLUSTERING; // 集群/广播

    // Broker 优先级
    private int brokerId = MixAll.MASTER_ID;

    // 5.x 新增
    private boolean enableSubscriptionMeta = true; // 是否启用订阅元数据
}
```

### 核心操作

**1. 创建/更新订阅组**

```java
public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
    String groupName = config.getGroupName();

    SubscriptionGroupConfig old = this.subscriptionGroupTable.put(groupName, config);
    if (old != null) {
        log.info("update subscription group config, old: {} new: {}", old, config);
    } else {
        log.info("create subscription group config, {}", config);
    }

    this.dataVersion.nextVersion();
    this.persist();
}
```

**2. 查询订阅组**

```java
public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
    SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(group);
    if (subscriptionGroupConfig == null) {
        // 如果不存在且允许自动创建，返回默认配置
        if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || MixAll.isSysConsumerGroup(group)) {
            subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(group);
        }
    }
    return subscriptionGroupConfig;
}
```

**3. 禁止订阅**

```java
public void forbiddenSub(String topic, int groupForbidden) {
    ConcurrentMap<String, Integer> groupMap = forbiddenTable.get(topic);
    if (groupMap == null) {
        groupMap = new ConcurrentHashMap<>();
        forbiddenTable.put(topic, groupMap);
    }
    groupMap.put(topic, groupForbidden);
}
```

## 🔄 与 NameServer 的同步

```
Broker 定时任务（每 30 秒）
    ↓
BrokerController.registerBrokerAll()
    ├── 获取最新的 TopicConfigTable
    ├── 获取最新的 SubscriptionGroupTable
    └── 发送请求到所有 NameServer
        └── RequestCode.REGISTER_BROKER

NameServer 收到注册请求
    ├── 更新 Topic 路由信息
    ├── 更新 Broker 信息
    └── 返回注册结果
```

## 📂 持久化文件

```
${storePath}/config/
├── topics.json              # TopicConfigTable
├── subscriptionGroup.json   # SubscriptionGroupTable
├── consumerOffset.json      # 消费进度
├── delayOffset.json         # 延迟消息进度
└── consumerFilter.json      # 消费过滤器
```

### JSON 格式示例

```json
{
  "dataVersion": {
    "timestamp": 1704067200000,
    "counter": 42
  },
  "topicConfigTable": {
    "TopicTest": {
      "topicName": "TopicTest",
      "readQueueNums": 8,
      "writeQueueNums": 8,
      "perm": 6,
      "topicFilterType": "SINGLE_TAG",
      "topicMessageType": "NORMAL",
      "cleanupPolicy": "DELETE"
    }
  }
}
```

## 💡 设计亮点

1. **ConcurrentHashMap**：高并发读，低争用写
2. **DataVersion**：版本号机制，NameServer 通过版本号判断是否需要更新
3. **自动创建**：`autoCreateSubscriptionGroup` / `autoCreateTopicEnable` 支持自动创建
4. **ReentrantLock + 超时**：创建 Topic 时加锁，防止死锁
5. **系统 Topic 保护**：内置 Topic 不能被删除或修改

## ⚠️ 注意事项

1. **自动创建要谨慎**：生产环境建议关闭自动创建，避免误创建
2. **队列数变更**：变更 readQueueNums/writeQueueNums 需要重启才生效
3. **持久化时机**：变更后立即持久化，但 NameServer 同步是定时的
4. **并发创建**：多个客户端同时创建同一 Topic 时，只保留第一个

## 🔗 与其他模块的关系

- **AdminBrokerProcessor**：管理命令创建/删除 Topic 和订阅组
- **NameServer**：Broker 定期同步 Topic/订阅组信息到 NameServer
- **SendMessageProcessor**：发送消息时检查 Topic 是否存在
- **PullMessageProcessor**：拉取消息时检查订阅组
- **Rebalance**：客户端 Rebalance 时查询订阅组配置

---

#核心类 #设计模式
