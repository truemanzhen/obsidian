# 消息追踪（Trace）

> 源码位置：`client/src/main/java/org/apache/rocketmq/client/trace/`

## 🏗️ 整体架构

RocketMQ 的 Trace 机制用于追踪消息的**全生命周期**：发送 → 存储 → 消费。

```
┌─────────────────────────────────────────────────────────┐
│                    Trace 体系                            │
├─────────────────────────────────────────────────────────┤
│  Producer 端                                            │
│  ┌──────────────┬───────────────────────────────┐      │
│  │ SendMessage   │ SendMessageTraceHookImpl      │      │
│  │ (业务发送)    │ (自动记录发送 Trace)          │      │
│  └──────────────┴───────────────────────────────┘      │
├─────────────────────────────────────────────────────────┤
│  Consumer 端                                            │
│  ┌──────────────┬───────────────────────────────┐      │
│  │ ConsumeMessage│ ConsumeMessageTraceHookImpl   │      │
│  │ (业务消费)    │ (自动记录消费 Trace)          │      │
│  └──────────────┴───────────────────────────────┘      │
├─────────────────────────────────────────────────────────┤
│  AsyncTraceDispatcher                                   │
│  ┌──────────────────────────────────────────────┐      │
│  │ 异步批量发送 Trace 数据到 Trace Topic         │      │
│  └──────────────────────────────────────────────┘      │
├─────────────────────────────────────────────────────────┤
│  Trace Topic: RMQ_SYS_TRACE_TOPIC                       │
│  └── 独立 Topic 存储所有 Trace 数据                      │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `AsyncTraceDispatcher` | **核心**：异步批量分发 Trace 数据 |
| `TraceContext` | Trace 上下文（一次追踪的完整信息） |
| `TraceBean` | Trace 数据单元（单条消息的追踪信息） |
| `TraceType` | 追踪类型枚举 |
| `TraceDataEncoder` | Trace 数据编解码 |
| `SendMessageTraceHookImpl` | 发送消息 Trace Hook |
| `ConsumeMessageTraceHookImpl` | 消费消息 Trace Hook |
| `TraceDispatcher` | Trace 分发器接口 |

## 📊 TraceType（追踪类型）

```java
public enum TraceType {
    Pub,           // 消息发送
    Recall,        // 消息撤回
    SubBefore,     // 消费前（收到消息）
    SubAfter,      // 消费后（处理完成）
    EndTransaction // 事务结束
}
```

## 📝 TraceContext & TraceBean

### TraceContext

```java
public class TraceContext {
    private TraceType traceType;      // 追踪类型
    private long timeStamp;           // 时间戳
    private String regionId;          // 区域 ID
    private String regionName;        // 区域名称
    private String groupName;         // 生产者/消费者组名
    private int costTime;             // 耗时（毫秒）
    private boolean isSuccess;        // 是否成功
    private String requestId;         // 请求 ID
    private AccessChannel accessChannel; // LOCAL/CLOUD
    private List<TraceBean> traceBeans;  // 消息列表
}
```

### TraceBean

```java
public class TraceBean {
    private String topic;          // Topic
    private String msgId;          // 消息 ID
    private String tags;           // Tag
    private String keys;           // Key
    private String storeHost;      // Broker 地址
    private int bodyLength;        // 消息体长度
    private long offsetMsgId;      // 偏移量消息 ID
    private long storeSize;        // 存储大小
    private int retryTimes;        // 重试次数
    private int queueId;           // 队列 ID
    private long queueOffset;      // 队列偏移量
}
```

## 📤 发送 Trace 流程

```
Producer.send(message)
    ↓
SendMessageTraceHookImpl.sendMessageBefore(context)
    ├── 创建 TraceContext (type=Pub)
    ├── 填充 TraceBean (topic, msgId, tags, keys...)
    └── AsyncTraceDispatcher.append(context)

Producer.send(message) 返回
    ↓
SendMessageTraceHookImpl.sendMessageAfter(context)
    ├── 设置 costTime（耗时）
    ├── 设置 isSuccess（是否成功）
    └── AsyncTraceDispatcher.append(context)
```

## 📥 消费 Trace 流程

```
Consumer 收到消息
    ↓
ConsumeMessageTraceHookImpl.consumeMessageBefore(context)
    ├── 创建 TraceContext (type=SubBefore)
    ├── 填充 TraceBean (topic, msgId, queueOffset...)
    └── AsyncTraceDispatcher.append(context)

Consumer 消费完成
    ↓
ConsumeMessageTraceHookImpl.consumeMessageAfter(context)
    ├── 创建 TraceContext (type=SubAfter)
    ├── 设置 costTime（消费耗时）
    ├── 设置 isSuccess（消费是否成功）
    └── AsyncTraceDispatcher.append(context)
```

## 🚀 AsyncTraceDispatcher（异步分发）

### 设计要点

```java
public class AsyncTraceDispatcher implements TraceDispatcher {
    // Trace 数据队列
    private final ArrayBlockingQueue<TraceContext> traceContextQueue;

    // 批量发送线程池
    private final ThreadPoolExecutor traceExecutor;

    // Trace 专用 Producer
    private final DefaultMQProducer traceProducer;

    // 批量参数
    private final int batchNum;      // 每批条数
    private final int maxMsgSize;    // 最大消息大小

    // 丢弃计数（队列满时）
    private AtomicLong discardCount;
}
```

### 异步发送流程

```
1. append(context)
   └── traceContextQueue.offer(context)
       ├── 成功 → 返回
       └── 队列满 → 丢弃 + discardCount++

2. Worker 线程（后台）
   ├── 从 traceContextQueue 批量取出
   ├── TraceDataEncoder 编码为字符串
   ├── 构建 Trace 消息
   │   ├── Topic: RMQ_SYS_TRACE_TOPIC
   │   ├── Body: 编码后的 Trace 数据
   │   └── Keys: msgId 列表
   └── traceProducer.send(traceMessage)
       └── 发送到 Trace Topic
```

### 批量编码

```java
// TraceDataEncoder 编码格式
// 每条 Trace 用 FIELD_SPLITOR 分隔
// 每个字段用 CONTENT_SPLITOR 分隔

// Pub 类型编码：
// Pub|timestamp|regionId|groupName|topic|msgId|tags|keys|storeHost|bodyLength|costTime

// SubAfter 类型编码：
// SubAfter|timestamp|regionId|groupName|msgId|tags|keys|storeHost|bodyLength|costTime|isSuccess
```

## 📊 Trace Topic

```java
// Trace 数据发送到专用 Topic
TopicValidator.RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC"

// 或者自定义 Trace Topic
producer.setTraceTopicName("CUSTOM_TRACE_TOPIC");
```

## 🔌 OpenTracing 集成

5.5.0 还提供了 OpenTracing 标准接口：

```java
// OpenTracing 兼容 Hook
SendMessageOpenTracingHookImpl        // 使用 OpenTracing API
ConsumeMessageOpenTracingHookImpl      // 使用 OpenTracing API
EndTransactionOpenTracingHookImpl      // 使用 OpenTracing API
```

## 💡 设计亮点

1. **异步无侵入**：Trace 数据异步发送，不影响业务性能
2. **批量发送**：减少网络开销
3. **队列满时丢弃**：保证业务优先，Trace 可以丢失
4. **Hook 机制**：通过 RPCHook 自动注入，业务无感知
5. **独立 Topic**：Trace 数据和业务数据隔离
6. **OpenTracing 兼容**：支持对接 Jaeger、Zipkin 等

## ⚠️ 注意事项

1. **Trace Topic 需要提前创建**
2. **Trace 会增加存储开销**（约 5-10%）
3. **丢弃风险**：队列满时 Trace 数据会丢失
4. **生产者组名冲突**：Trace Producer 使用特殊组名避免冲突

## 🔗 与其他模块的关系

- **Client**：Trace Hook 在客户端自动注册
- **Broker**：Trace 数据发送到 Broker 的 Trace Topic
- **Admin**：可以通过 Admin 工具查询 Trace 数据
- **OpenTracing**：支持对接外部链路追踪系统

---

#核心类 #设计模式
