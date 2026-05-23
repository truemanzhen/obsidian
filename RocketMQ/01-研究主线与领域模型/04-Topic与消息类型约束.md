# Topic 与消息类型约束

> 研究定位：把 `Topic` 从“名字”提升为“属性 + 清理策略 + 队列类型 + 消息语义”的资源。
> 关键源码：`common/src/main/java/org/apache/rocketmq/common/TopicAttributes.java`、`common/src/main/java/org/apache/rocketmq/common/TopicConfig.java`、`common/src/main/java/org/apache/rocketmq/common/utils/QueueTypeUtils.java`、`common/src/main/java/org/apache/rocketmq/common/utils/CleanupPolicyUtils.java`
> 阅读建议：先看本篇，再接 [[45-TopicMessageType校验与路由行为]]、[[52-LiteMode与LiteTopic]]。

## 先给结论

- `Topic` 已经不是单纯的名字。
- 5.5.0 里，`TopicConfig.attributes` 承载了消息类型、队列类型、清理策略、保留时长和 Lite 过期时间。
- 真正起作用的不是“字段存在”，而是“谁读取它、在哪一层校验它、是否允许更新它”。

## 属性总表

源码里的完整属性集合如下：

```java
public static final EnumAttribute QUEUE_TYPE_ATTRIBUTE = new EnumAttribute(
    "queue.type", false, newHashSet("BatchCQ", "SimpleCQ"), "SimpleCQ");
public static final EnumAttribute CLEANUP_POLICY_ATTRIBUTE = new EnumAttribute(
    "cleanup.policy", false, newHashSet("DELETE", "COMPACTION"), "DELETE");
public static final EnumAttribute TOPIC_MESSAGE_TYPE_ATTRIBUTE = new EnumAttribute(
    "message.type", true, TopicMessageType.topicMessageTypeSet(), TopicMessageType.NORMAL.getValue());
public static final LongRangeAttribute TOPIC_RESERVE_TIME_ATTRIBUTE = new LongRangeAttribute(
    "reserve.time", true, -1, Long.MAX_VALUE, -1);
public static final LongRangeAttribute LITE_EXPIRATION_ATTRIBUTE = new LongRangeAttribute(
    "lite.topic.expiration", true, -1, TimeUnit.DAYS.toMinutes(30), -1);
```

## 这几个属性分别干什么

- `message.type`：Topic 级消息契约。
- `queue.type`：ConsumeQueue 的实现选择。
- `cleanup.policy`：删除还是 compaction。
- `reserve.time`：tiered storage 下的保留时长。
- `lite.topic.expiration`：LiteTopic 生命周期。

## 关键源码

### 1. TopicConfig 把属性挂到同一个 map

```java
public TopicMessageType getTopicMessageType() {
    if (attributes == null) {
        return TopicMessageType.NORMAL;
    }
    String content = attributes.get(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName());
    if (content == null) {
        return TopicMessageType.NORMAL;
    }
    return TopicMessageType.valueOf(content);
}

public void setTopicMessageType(TopicMessageType topicMessageType) {
    attributes.put(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), topicMessageType.getValue());
}
```

### 2. queue.type 不是任意值

```java
public enum CQType {
    SimpleCQ,
    BatchCQ,
    RocksDBCQ
}
```

```java
public static CQType getCQType(Optional<TopicConfig> topicConfig) {
    if (!topicConfig.isPresent()) {
        return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
    }
    ...
    return CQType.valueOf(attributes.get(attributeName));
}
```

注意：

- `TopicAttributes.QUEUE_TYPE_ATTRIBUTE` 的允许值只有 `SimpleCQ` 和 `BatchCQ`。
- `RocksDBCQ` 是运行时存储实现语义，不是普通 topic 属性值。

### 3. cleanup.policy 的读取逻辑是工具函数驱动

```java
public static CleanupPolicy getDeletePolicy(Optional<TopicConfig> topicConfig) {
    if (!topicConfig.isPresent()) {
        return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
    }
    ...
    if (attributes.containsKey(attributeName)) {
        return CleanupPolicy.valueOf(attributes.get(attributeName));
    }
    return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
}
```

### 4. reserve.time 只在 tiered store 里允许更新

```java
if (!(brokerController.getMessageStore() instanceof TieredMessageStore)) {
    if (newAttributes.get(TopicAttributes.TOPIC_RESERVE_TIME_ATTRIBUTE.getName()) != null) {
        throw new IllegalArgumentException("Update topic reserveTime not supported");
    }
    return;
}
```

## 这意味着什么

- 不是所有 topic 属性都对所有存储模式有效。
- 不是所有属性都在 broker 启动时同等生效。
- 有些属性只是“配置入口”，真正生效还要看 Broker / Store 的支撑能力。

## 研究时要补的知识

- `TopicAttributes.ALL` 是 broker 更新 topic 配置时的白名单入口。
- `TopicMessageType` 的校验和路由暴露见 。
- `reserve.time` 更像 tiered storage 的元数据，而不是普通 broker 的通用 topic 属性。
- `cleanup.policy=COMPACTION` 对应的不是“普通删除”，而是按 key 保留最新值的存储语义。

## 建议一起看