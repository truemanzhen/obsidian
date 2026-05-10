# Admin 工具与运维命令

> 源码位置：`broker/src/main/java/org/apache/rocketmq/broker/processor/AdminBrokerProcessor.java` + `tools/`

## 🏗️ 整体架构

Admin 工具是运维管理 RocketMQ 的核心入口，通过 `AdminBrokerProcessor` 处理管理命令。

```
┌─────────────────────────────────────────────────────────┐
│                   Admin 工具体系                          │
├──────────────┬──────────────┬────────────────────────────┤
│ mqadmin CLI  │ Admin API    │ AdminBrokerProcessor       │
│ (命令行)     │ (Java API)   │ (Broker 侧处理)           │
├──────────────┴──────────────┴────────────────────────────┤
│              Remoting 协议                               │
│         RequestCode.ADMIN_* → Broker                    │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `AdminBrokerProcessor` | Broker 侧管理命令处理器 |
| `MQAdminImpl` | 客户端 Admin 实现 |
| `AdminTool` | 命令行工具入口 |
| `CommandUtil` | 命令工具类 |

## 📋 管理命令分类

### Topic 管理

| 命令 | RequestCode | 功能 |
|---|---|---|
| `updateTopic` | UPDATE_AND_CREATE_TOPIC | 创建/更新 Topic |
| `deleteTopic` | DELETE_TOPIC_IN_BROKER | 删除 Topic |
| `topicList` | GET_ALL_TOPIC_CONFIG | 获取所有 Topic |
| `topicStatus` | GET_TOPIC_STATS_INFO | 获取 Topic 统计 |
| `topicRoute` | GET_TOPIC_ROUTE_INFO | 获取 Topic 路由 |

### 消费组管理

| 命令 | RequestCode | 功能 |
|---|---|---|
| `updateSubGroup` | UPDATE_AND_CREATE_SUBSCRIPTIONGROUP | 创建/更新订阅组 |
| `deleteSubGroup` | DELETE_SUBSCRIPTIONGROUP | 删除订阅组 |
| `consumerStatus` | GET_CONSUMER_CONNECTION_LIST | 获取消费者连接 |
| `getConsumerStatus` | INVOKE_BROKER_TO_GET_CONSUMER_STATUS | 获取消费状态 |
| `resetOffsetByTime` | INVOKE_BROKER_TO_RESET_OFFSET | 按时间重置 Offset |

### 消息查询

| 命令 | RequestCode | 功能 |
|---|---|---|
| `queryMsgById` | VIEW_MESSAGE_BY_ID | 按消息 ID 查询 |
| `queryMsgByKey` | QUERY_MESSAGE | 按 Key 查询 |
| `queryMsgByOffset` | - | 按 Offset 查询 |
| `queryMsgByUniqueKey` | - | 按 UniqueKey 查询 |

### Broker 管理

| 命令 | RequestCode | 功能 |
|---|---|---|
| `updateBrokerConfig` | UPDATE_BROKER_CONFIG | 动态更新配置 |
| `getBrokerConfig` | GET_BROKER_CONFIG | 获取配置 |
| `brokerRuntimeInfo` | GET_BROKER_RUNTIME_INFO | 获取运行时信息 |
| `cleanExpiredCommitLog` | DELETE_EXPIRED_COMMITLOG | 清理过期 CommitLog |
| `cleanUnusedTopic` | CLEAN_UNUSED_TOPIC | 清理无用 Topic |

### Offset 管理

| 命令 | RequestCode | 功能 |
|---|---|---|
| `queryConsumerOffset` | QUERY_CONSUMER_OFFSET | 查询消费进度 |
| `updateConsumerOffset` | UPDATE_CONSUMER_OFFSET | 更新消费进度 |
| `getAllConsumerOffset` | GET_ALL_CONSUMER_OFFSET | 获取所有消费进度 |
| `resetOffsetByTime` | INVOKE_BROKER_TO_RESET_OFFSET | 按时间重置 |

## 🔧 AdminBrokerProcessor 处理流程

```java
@Override
public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
    switch (request.getCode()) {
        case RequestCode.UPDATE_AND_CREATE_TOPIC:
            return updateTopic(ctx, request);
        case RequestCode.DELETE_TOPIC_IN_BROKER:
            return deleteTopic(ctx, request);
        case RequestCode.GET_ALL_TOPIC_CONFIG:
            return getAllTopicConfig(ctx, request);
        // ... 40+ 个 case
        case RequestCode.GET_BROKER_RUNTIME_INFO:
            return getBrokerRuntimeInfo(ctx, request);
        default:
            return RemotingCommand.createResponseCommand(
                RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                "request type " + request.getCode() + " not supported");
    }
}
```

## 📊 常用运维场景

### 1. 动态调整日志级别

```bash
# 通过 updateBrokerConfig 动态修改
mqadmin updateBrokerConfig -b 192.168.1.1:10911 -k logLevel -v DEBUG
```

### 2. 按时间重置消费进度

```bash
# 重置到指定时间点
mqadmin resetOffsetByTime -t TopicTest -g ConsumerGroup -s 2024-01-01#00:00:00

# 重置到最大 Offset（跳过所有消息）
mqadmin resetOffsetByTime -t TopicTest -g ConsumerGroup -s now
```

### 3. 查看 Broker 运行状态

```bash
mqadmin brokerRuntimeInfo -b 192.168.1.1:10911
# 输出：
# bootTimestamp: 2024-01-01 00:00:00
# runtimeSinceLastSeconds: 86400
# sendThreadPoolQueueSize: 0
# pullThreadPoolQueueSize: 0
# putMessageDistributeTime: ...
```

### 4. 查询消息轨迹

```bash
# 按消息 ID 查询
mqadmin queryMsgById -i C0A8010100002A9F0000000000000001

# 按 Key 查询
mqadmin queryMsgByKey -t TopicTest -k OrderId12345
```

## 💡 设计亮点

1. **统一处理入口**：所有管理命令通过 `AdminBrokerProcessor` 分发
2. **动态生效**：大部分配置修改不需要重启
3. **集群广播**：管理命令会广播到所有 Broker
4. **权限控制**：管理命令需要 ACL 授权
5. **完整审计**：管理操作记录到审计日志

## ⚠️ 注意事项

1. **生产环境慎用**：resetOffset、deleteTopic 等操作不可逆
2. **批量操作**：大量 Topic/Group 操作建议脚本化
3. **超时设置**：管理命令可能需要较长超时时间
4. **权限管理**：管理命令需要 ADMIN 权限

## 🔗 与其他模块的关系

- **TopicConfigManager**：Admin 命令操作 Topic 配置
- **SubscriptionGroupManager**：Admin 命令操作订阅组
- **ConsumerOffsetManager**：Admin 命令操作消费进度
- **NameServer**：Admin 命令可能需要查询路由信息

---

#核心类
