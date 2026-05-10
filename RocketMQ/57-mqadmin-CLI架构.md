# mqadmin CLI 架构

> 源码位置：`tools/src/main/java/org/apache/rocketmq/tools/command/`

## 🏗️ 整体架构

mqadmin 是 RocketMQ 的**命令行运维工具**，通过 SubCommand 模式组织 100+ 个子命令。

```
┌─────────────────────────────────────────────────────────┐
│  mqadmin <command> [options]                             │
├─────────────────────────────────────────────────────────┤
│  MQAdminStartup (入口)                                  │
│  ├── 解析命令行参数                                      │
│  ├── 匹配 SubCommand                                    │
│  └── execute(commandLine, options, rpcHook)              │
├─────────────────────────────────────────────────────────┤
│  SubCommand 接口                                         │
│  ├── commandName() → 命令名                              │
│  ├── commandAlias() → 别名                               │
│  ├── commandDesc() → 描述                                │
│  ├── buildCommandlineOptions() → 构建选项                │
│  └── execute() → 执行                                   │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `MQAdminStartup` | CLI 入口，注册所有 SubCommand |
| `SubCommand` | 子命令接口 |
| `CommandUtil` | 命令工具类（连接 Broker/NameServer） |

## 📝 SubCommand 接口

```java
public interface SubCommand {
    String commandName();                              // 命令名
    default String commandAlias() { return null; }     // 别名
    String commandDesc();                              // 描述
    Options buildCommandlineOptions(Options options);  // 构建 CLI 选项
    void execute(CommandLine cmd, Options options,
        RPCHook rpcHook) throws SubCommandException;   // 执行
}
```

## 📋 命令分类

### 集群管理
| 命令 | 功能 |
|---|---|
| `clusterList` | 查看集群列表 |
| `brokerStatus` | Broker 状态 |
| `getBrokerEpoch` | 获取 Broker Epoch |

### Topic 管理
| 命令 | 功能 |
|---|---|
| `updateTopic` | 创建/更新 Topic |
| `deleteTopic` | 删除 Topic |
| `topicList` | 列出所有 Topic |
| `topicRoute` | 查看路由 |
| `topicStatus` | 查看状态 |
| `topicClusterList` | Topic 所在集群 |

### 消费组管理
| 命令 | 功能 |
|---|---|
| `updateSubGroup` | 创建/更新消费组 |
| `deleteSubGroup` | 删除消费组 |
| `consumerProgress` | 消费进度 |
| `consumerStatus` | 消费者状态 |
| `cloneGroupOffset` | 克隆消费进度 |

### 消息管理
| 命令 | 功能 |
|---|---|
| `queryMsgById` | 按 ID 查询 |
| `queryMsgByKey` | 按 Key 查询 |
| `queryMsgByUniqueKey` | 按 UniqueKey 查询 |
| `queryMsgByOffset` | 按 Offset 查询 |
| `printMsg` | 打印消息 |
| `printMsgByQueue` | 按队列打印 |

### Offset 管理
| 命令 | 功能 |
|---|---|
| `resetOffsetByTime` | 按时间重置 |
| `updateOffset` | 更新 Offset |
| `getConsumerStatus` | 获取消费状态 |

### 配置管理
| 命令 | 功能 |
|---|---|
| `updateBrokerConfig` | 更新 Broker 配置 |
| `getBrokerConfig` | 获取配置 |
| `cleanExpiredCQ` | 清理过期 CQ |
| `cleanUnusedTopic` | 清理无用 Topic |

### ACL 管理
| 命令 | 功能 |
|---|---|
| `createAcl` | 创建 ACL |
| `deleteAcl` | 删除 ACL |
| `listAcl` | 列出 ACL |
| `createUser` | 创建用户 |
| `getAcl` | 获取 ACL 详情 |

## 🔧 命令注册

```java
public class MQAdminStartup {
    private static List<SubCommand> subCommands = new ArrayList<>();

    static {
        // 注册所有子命令
        subCommands.add(new ClusterListSubCommand());
        subCommands.add(new TopicListSubCommand());
        subCommands.add(new TopicRouteSubCommand());
        subCommands.add(new UpdateTopicSubCommand());
        subCommands.add(new ConsumerProgressSubCommand());
        subCommands.add(new QueryMsgByIdSubCommand());
        subCommands.add(new ResetOffsetByTimeSubCommand());
        // ... 100+ 个命令
    }

    public static void main(String[] args) {
        // 1. 解析全局选项（-n NameServer, -c Cluster）
        // 2. 匹配子命令
        // 3. 执行
        subCommand.execute(commandLine, options, rpcHook);
    }
}
```

## 💡 设计亮点

1. **SubCommand 模式**：每个命令独立实现，易于扩展
2. **alias 支持**：命令支持别名（如 `updateTopic` / `ut`）
3. **统一 RPC Hook**：支持 ACL 认证
4. **Options 复用**：Apache Commons CLI 构建选项

## 🔗 与其他模块的关系

- **AdminBrokerProcessor**：mqadmin 命令最终由 Broker 的 AdminBrokerProcessor 处理
- **NameServer**：部分命令需要先查询路由
- **ACL**：所有命令支持 `-a AK -s SK` 认证

---

#核心类
