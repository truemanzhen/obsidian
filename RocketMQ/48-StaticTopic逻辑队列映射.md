# StaticTopic 逻辑队列映射

> 源码位置：`remoting/protocol/statictopic/` + `broker/topic/TopicQueueMappingManager.java`

## 🏗️ 什么是 StaticTopic

StaticTopic 是 5.x 引入的**逻辑队列映射**机制，解决传统 Topic 物理队列**无法扩容**的问题。

```
传统 Topic：
┌─────────────────────────────────────┐
│  Topic-A (writeQueueNums=4)         │
│  ├── Queue-0 (Broker-1)             │
│  ├── Queue-1 (Broker-1)             │
│  ├── Queue-2 (Broker-2)             │
│  └── Queue-3 (Broker-2)             │
│  问题：无法动态增加队列              │
└─────────────────────────────────────┘

StaticTopic：
┌─────────────────────────────────────┐
│  Topic-A (逻辑队列 8 个)            │
│  ├── LogicQueue-0 → Broker-1 物理Q  │
│  ├── LogicQueue-1 → Broker-1 物理Q  │
│  ├── ...                            │
│  ├── LogicQueue-4 → Broker-3 物理Q  │  ← 可以动态映射到新 Broker
│  └── LogicQueue-7 → Broker-3 物理Q  │
│  支持：动态扩容、跨 Broker 分布      │
└─────────────────────────────────────┘
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│              TopicQueueMapping 体系                      │
├─────────────────────────────────────────────────────────┤
│  TopicQueueMappingManager (Broker 侧管理)               │
│  ├── topicQueueMappingTable (Topic → MappingDetail)     │
│  ├── buildTopicQueueMappingContext()                    │
│  └── rewriteRequestForStaticTopic()                     │
├─────────────────────────────────────────────────────────┤
│  TopicQueueMappingDetail (映射详情)                      │
│  ├── globalId (逻辑队列 ID)                             │
│  ├── totalQueues (总逻辑队列数)                          │
│  └── items[] (LogicQueueMappingItem 列表)               │
├─────────────────────────────────────────────────────────┤
│  LogicQueueMappingItem (单条映射)                        │
│  ├── logicQueueId (逻辑队列 ID)                         │
│  ├── brokerName (目标 Broker)                           │
│  ├── queueId (物理队列 ID)                              │
│  ├── startOffset / endOffset (偏移量范围)                │
│  └── timeOfStart / timeOfEnd (时间范围)                  │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `TopicQueueMappingManager` | Broker 侧映射管理器 |
| `TopicQueueMappingDetail` | 映射详情（一个 Topic 的完整映射） |
| `LogicQueueMappingItem` | 单条映射记录 |
| `TopicQueueMappingContext` | 映射上下文（请求处理时传递） |
| `TopicQueueMappingUtils` | 映射工具类 |
| `TopicQueueMappingOne` | 单个队列的映射信息 |

## 📝 LogicQueueMappingItem

```java
public class LogicQueueMappingItem {
    private int globalId;        // 逻辑队列 ID
    private String brokerName;   // 目标 Broker 名称
    private int queueId;         // 物理队列 ID
    private long startOffset;    // 起始偏移量
    private long endOffset;      // 结束偏移量（-1 表示未结束）
    private long timeOfStart;    // 起始时间戳
    private long timeOfEnd;      // 结束时间戳
}
```

**示例：**
```
LogicQueue-0 的映射历史：
Item-0: Broker-1, Queue-0, offset=0~10000, time=0~1704067200000
Item-1: Broker-3, Queue-0, offset=0~, time=1704067200000~  ← 当前活跃
```

## 🔄 请求重写流程

### 发送消息

```
Producer 发送消息到 LogicQueue-4
    ↓
SendMessageProcessor.processRequest()
    ├── TopicQueueMappingManager.buildTopicQueueMappingContext()
    │   ├── 查询 LogicQueue-4 的映射关系
    │   └── 找到当前活跃的 LogicQueueMappingItem
    ├── rewriteRequestForStaticTopic()
    │   ├── 将 LogicQueueId 替换为物理 QueueId
    │   ├── 更新 BrokerName
    │   └── 如果需要转发到其他 Broker → 返回重定向
    └── 写入物理队列
```

### 拉取消息

```
Consumer 拉取 LogicQueue-4
    ↓
PullMessageProcessor.processRequest()
    ├── 查询 LogicQueue-4 的映射
    ├── 读取物理队列数据
    ├── 重写响应中的 offset
    │   └── 物理 offset → 逻辑 offset
    └── 返回给消费者
```

## 📈 扩容流程

```
初始状态（4 个逻辑队列分布在 2 个 Broker）：
LogicQueue-0,1 → Broker-1
LogicQueue-2,3 → Broker-2

扩容到 8 个逻辑队列（新增 Broker-3）：
LogicQueue-0,1 → Broker-1（不变）
LogicQueue-2,3 → Broker-2（不变）
LogicQueue-4,5 → Broker-3（新增）
LogicQueue-6,7 → Broker-3（新增）
```

### 映射更新

```java
// 更新映射关系
TopicQueueMappingDetail detail = new TopicQueueMappingDetail();
detail.setTotalQueues(8);
detail.getItems().add(new LogicQueueMappingItem(4, "Broker-3", 0, 0, -1, now, -1));
detail.getItems().add(new LogicQueueMappingItem(5, "Broker-3", 1, 0, -1, now, -1));

// 持久化到所有 Broker
topicQueueMappingManager.updateTopicQueueMapping(detail);
```

## 💡 设计亮点

1. **逻辑物理分离**：逻辑队列 ID 与物理队列 ID 解耦
2. **映射历史**：每个逻辑队列可以有多条映射记录（支持迁移）
3. **请求重写**：对 Producer/Consumer 透明，无感知迁移
4. **跨 Broker 分布**：逻辑队列可以分布在任意 Broker
5. **时间维度**：支持按时间范围映射（用于数据迁移）

## ⚠️ 注意事项

1. **全局一致性**：所有 Broker 必须持有相同的映射信息
2. **迁移复杂度**：跨 Broker 迁移需要保证数据一致性
3. **ConsumeQueue**：StaticTopic 的 ConsumeQueue 需要特殊处理
4. **Admin 工具**：需要专用命令管理 StaticTopic

## 🔗 与其他模块的关系

- **TopicConfigManager**：StaticTopic 与普通 Topic 配置共存
- **SendMessageProcessor**：发送时重写请求
- **PullMessageProcessor**：拉取时重写响应
- **NameServer**：路由信息包含映射关系
- **Admin**：管理 StaticTopic 的映射配置

---

#设计模式 #源码技巧
