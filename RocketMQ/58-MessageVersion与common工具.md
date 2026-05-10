# MessageVersion 编码版本与 common 工具

> 源码位置：`common/src/main/java/org/apache/rocketmq/common/`

## 📊 MessageVersion V1 vs V2

5.x 引入了 V2 版本的消息编码格式，主要区别在于 **Topic 长度字段**。

### V1 编码（默认）

```
┌──────────────────────────────────────────────┐
│  Topic Length: 1 byte (最大 255 字符)         │
│  Topic: N bytes                               │
│  Properties Length: 2 bytes                   │
│  Properties: M bytes                          │
└──────────────────────────────────────────────┘
Magic Code: -626843481
```

### V2 编码

```
┌──────────────────────────────────────────────┐
│  Topic Length: 2 bytes (最大 65535 字符)      │
│  Topic: N bytes                               │
│  Properties Length: 2 bytes                   │
│  Properties: M bytes                          │
└──────────────────────────────────────────────┘
Magic Code: -626843477
```

### 核心差异

```java
public enum MessageVersion {
    MESSAGE_VERSION_V1(MessageDecoder.MESSAGE_MAGIC_CODE) {
        @Override
        public int getTopicLengthSize() { return 1; }     // 1 byte
        @Override
        public int getTopicLength(ByteBuffer buffer) {
            return buffer.get();                           // 读 1 byte
        }
    },
    MESSAGE_VERSION_V2(MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
        @Override
        public int getTopicLengthSize() { return 2; }     // 2 bytes
        @Override
        public int getTopicLength(ByteBuffer buffer) {
            return buffer.getShort();                      // 读 2 bytes
        }
    };
}
```

### 为什么需要 V2

- V1 的 Topic 长度只有 1 byte，最长 255 字符
- 5.x 支持 Namespace + Topic，总长度可能超过 255
- V2 用 2 bytes，最长 65535 字符

---

## 📦 ConfigManager 持久化框架

Broker 的所有配置管理器（TopicConfigManager、ConsumerOffsetManager 等）都继承自 `ConfigManager`。

### 核心机制

```java
public abstract class ConfigManager {
    // 子类必须实现
    public abstract String configFilePath();     // 配置文件路径
    public abstract String encode();             // 序列化为 JSON
    public abstract void decode(String json);    // 从 JSON 反序列化

    // 加载配置
    public boolean load() {
        String fileName = this.configFilePath();
        String jsonString = MixAll.file2String(fileName);

        if (jsonString == null || jsonString.isEmpty()) {
            // 主文件为空，尝试加载备份
            return this.loadBak();
        } else {
            this.decode(jsonString);
            return true;
        }
    }

    // 持久化配置
    public synchronized void persist() {
        String jsonString = this.encode();
        // 先写 .bak 文件（防止写入中途崩溃）
        MixAll.string2File(jsonString, this.configFilePath() + ".bak");
        // 再写主文件
        MixAll.string2File(jsonString, this.configFilePath());
    }
}
```

### 继承体系

```
ConfigManager (抽象基类)
  ├── TopicConfigManager          → topics.json
  ├── SubscriptionGroupManager    → subscriptionGroup.json
  ├── ConsumerOffsetManager       → consumerOffset.json
  ├── TopicQueueMappingManager    → topicQueueMapping.json
  └── ConsumerFilterManager       → consumerFilter.json
```

### 备份机制

```
写入顺序：
1. 写 topics.json.bak（备份）
2. 写 topics.json（主文件）

读取顺序：
1. 读 topics.json
2. 如果失败 → 读 topics.json.bak
3. 如果都失败 → 返回空

目的：防止写入中途崩溃导致配置丢失
```

---

## 📦 KeyBuilder

构建各种内部 Key 的工具类。

```java
public class KeyBuilder {
    // Pop 重试 Topic
    public static String buildPopRetryTopic(String topic, String cid) {
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + "_" + topic;
        // 格式：%RETRY%ConsumerGroup_TopicName
    }

    // Pop 重试 Topic V2（用 + 分隔）
    public static String buildPopRetryTopicV2(String topic, String cid) {
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + "+" + topic;
    }

    // 构建消费进度 Key
    public static String buildConsumerProgressKey(String topic, String group) {
        return topic + "@" + group;
    }
}
```

---

## 📦 MixAll 常量

```java
public class MixAll {
    // 系统 Topic 前缀
    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    public static final String DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
    public static final String SYSTEM_TOPIC_PREFIX = "rmq_sys_";

    // 默认组名
    public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    public static final String DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";

    // 环境变量
    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";

    // 文件操作
    public static String file2String(String fileName);      // 文件 → 字符串
    public static void string2File(String str, String fileName);  // 字符串 → 文件
}
```

---

## 📦 ServiceThread（后台服务线程）

很多组件（如 ReputMessageService、PopReviveService）都继承自 ServiceThread。

```java
public abstract class ServiceThread extends Thread {
    protected volatile boolean hasNotified = false;
    protected volatile boolean stopped = false;
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    // 子类实现
    public abstract String getServiceName();
    public abstract void run();

    // 唤醒等待
    public void wakeup() {
        hasNotified = true;
        waitPoint.countDown();
    }

    // 等待（可被唤醒）
    protected void waitForRunning(long interval) {
        if (hasNotified) {
            hasNotified = false;
            return;
        }
        waitPoint.await(interval, TimeUnit.MILLISECONDS);
        hasNotified = false;
    }

    // 优雅停止
    public void shutdown() {
        this.stopped = true;
        wakeup();
        this.join();
    }
}
```

---

## 💡 设计亮点

1. **ConfigManager 备份机制**：.bak 文件防止写入崩溃丢数据
2. **ServiceThread 通知机制**：waitPoint + hasNotified 实现高效等待唤醒
3. **KeyBuilder 统一 Key**：避免各处硬编码 Key 格式
4. **MessageVersion 兼容**：V1/V2 通过 MagicCode 自动识别

## 🔗 与其他模块的关系

- **所有 Broker 管理器**：继承 ConfigManager
- **所有后台服务**：继承 ServiceThread
- **消息编码**：MessageVersion 影响 CommitLog 编码

---

#核心类 #源码技巧
