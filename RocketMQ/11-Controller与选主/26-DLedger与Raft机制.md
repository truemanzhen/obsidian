# DLedger 与 JRaft 机制 — Raft 共识在 RocketMQ 中的应用

> 📄 源码路径：`controller/src/main/java/org/apache/rocketmq/controller/impl/DLedgerController.java`
> 🏷️ #核心类 #设计模式 #面试高频

## 🎯 核心思想

RocketMQ 使用 **Raft 共识算法**保证分布式场景下的数据一致性和高可用。
5.5.0 中 Raft 用于两个场景：
1. **Controller 选主**：管理 Broker 主从切换（DLedger）
2. **CommitLog 复制**：多副本 CommitLog（DLedger Commitlog，可选）

## 📐 Raft 算法核心概念

```
┌──────────────────────────────────────────────────────────────┐
│                    Raft 核心概念                               │
│                                                              │
│  角色：                                                      │
│    Leader（领导者）：处理所有写入，复制日志给 Follower          │
│    Follower（追随者）：接收 Leader 的日志复制                  │
│    Candidate（候选者）：发起选举                              │
│                                                              │
│  任期（Term/Epoch）：                                        │
│    每次选举 Term+1                                           │
│    一个 Term 最多一个 Leader                                  │
│                                                              │
│  日志复制（Log Replication）：                                │
│    Leader 接收写入 → 追加到本地日志 → 复制给 Follower         │
│    多数派确认 → 提交（Commit）→ 应用到状态机                  │
│                                                              │
│  选举（Election）：                                          │
│    Follower 超时未收到心跳 → 变为 Candidate → 发起投票        │
│    获得多数派投票 → 成为 Leader                               │
└──────────────────────────────────────────────────────────────┘
```

## 📐 DLedger 在 RocketMQ 中的应用

### 1. Controller 模式 — 选主管理

```
┌──────────────────────────────────────────────────────────────┐
│              DLedgerController 架构                            │
│                                                              │
│  ┌─────────────────────────────────────────┐                 │
│  │ DLedger Server (Raft 节点)               │                 │
│  │                                         │                 │
│  │ ┌──────────────┐  ┌──────────────────┐ │                 │
│  │ │ DLedger      │  │ Controller       │ │                 │
│  │ │ LeaderElector│  │ StateMachine     │ │                 │
│  │ │ (Raft 选举)  │  │ (状态机)         │ │                 │
│  │ └──────────────┘  └──────────────────┘ │                 │
│  │                                         │                 │
│  │ ┌──────────────────────────────────┐   │                 │
│  │ │ ReplicasInfoManager               │   │                 │
│  │ │ - Broker 注册信息                  │   │                 │
│  │ │ - SyncStateSet                    │   │                 │
│  │ │ - 选主逻辑                         │   │                 │
│  │ └──────────────────────────────────┘   │                 │
│  └─────────────────────────────────────────┘                 │
│              ↑ Raft 共识                                      │
│  ┌───────────┴─────────────────────────────┐                 │
│  │ Controller Node-1 (Leader)              │                 │
│  │ Controller Node-2 (Follower)            │                 │
│  │ Controller Node-3 (Follower)            │                 │
│  └─────────────────────────────────────────┘                 │
└──────────────────────────────────────────────────────────────┘
```

### DLedgerController 核心代码

```java
public class DLedgerController implements Controller {
    private final DLedgerServer dLedgerServer;
    private final ReplicasInfoManager replicasInfoManager;
    private final DLedgerControllerStateMachine statemachine;
    private final EventScheduler scheduler;
    private final RoleChangeHandler roleHandler;

    // 通过 DLedger 共识写入事件
    public CompletableFuture<RemotingCommand> electMaster(ElectMasterRequestHeader request) {
        return scheduler.appendEvent("electMaster",
            () -> replicasInfoManager.electMaster(request, electPolicy),
            true);  // true = 需要 Raft 共识
    }

    public CompletableFuture<RemotingCommand> registerBroker(RegisterBrokerToControllerRequestHeader request) {
        return scheduler.appendEvent("registerSuccess",
            () -> replicasInfoManager.registerBroker(request, brokerAlivePredicate),
            true);
    }
}
```

### EventScheduler — 事件调度器

```java
// EventScheduler 将操作封装为 Event，通过 DLedger 共识
class EventScheduler {
    // 写事件（需要 Raft 共识）
    public CompletableFuture<RemotingCommand> appendEvent(String name,
        Supplier<ControllerResult<?>> eventSupplier, boolean isWrite) {

        if (isWrite) {
            // ① 序列化事件
            EventMessage event = eventSupplier.get();
            byte[] data = eventSerializer.serialize(event);

            // ② 通过 DLedger 共识写入
            AppendEntryRequest request = new AppendEntryRequest();
            request.setBody(data);
            AppendEntryResponse response = dLedgerServer.append(request);

            // ③ 多数派确认后返回
            return CompletableFuture.completedFuture(response);
        } else {
            // 读事件，直接执行
            return CompletableFuture.completedFuture(eventSupplier.get());
        }
    }
}
```

> [!important] 写操作通过 Raft 共识
> Controller 的写操作（electMaster、registerBroker、alterSyncStateSet）
> 都通过 DLedger 共识写入，保证**多数派确认**后才生效。
> 读操作（getReplicaInfo）直接读取本地状态机。

### RoleChangeHandler — 角色变更处理

```java
// DLedger 角色变更时的通知
class RoleChangeHandler {
    public void handle(long term, MemberState.Role role) {
        if (role == MemberState.Role.LEADER) {
            // 成为 Leader，开始处理请求
            isLeaderState = true;
            startScheduling();
        } else {
            // 变为 Follower，停止处理写请求
            isLeaderState = false;
            stopScheduling();
        }
    }
}
```

## 📐 DLedger Commitlog — 基于 Raft 的 CommitLog（可选）

```
┌──────────────────────────────────────────────────────────────┐
│              DLedger Commitlog 架构                            │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Broker-1     │  │ Broker-2     │  │ Broker-3     │       │
│  │ (Leader)     │  │ (Follower)   │  │ (Follower)   │       │
│  │              │  │              │  │              │       │
│  │ DLedger      │  │ DLedger      │  │ DLedger      │       │
│  │ CommitLog    │  │ CommitLog    │  │ CommitLog    │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│         ↑                ↑                ↑                  │
│         └────────────────┼────────────────┘                  │
│                    Raft 日志复制                               │
│                                                              │
│  Producer → Leader CommitLog → 复制到 Follower               │
│          → 多数派确认 → 返回 OK                               │
└──────────────────────────────────────────────────────────────┘
```

> [!note] DLedger Commitlog vs 传统 HA
> | | 传统 HA | DLedger Commitlog |
> |---|---|---|
> | 复制方式 | Master 推送给 Slave | Raft 日志复制 |
> | 一致性 | 同步/异步可选 | 多数派确认 |
> | 选主 | 手动/Controller | Raft 自动选主 |
> | 脑裂防护 | 无 | Raft 保证 |
> | 性能 | 较高 | 较低（共识开销） |

## 📐 JRaft 在 RocketMQ 中的应用

> [!note] JRaft vs DLedger
> RocketMQ 5.5.0 中主要使用 **DLedger**（Apache 开源的 Raft 实现）。
> JRaft 是另一个 Raft 实现（蚂蚁金服开源），在某些版本中也有使用。
>
> 两者都是 Raft 的 Java 实现，核心算法相同：
> - Leader 选举
> - 日志复制
> - 安全性保证

### Raft 在 RocketMQ 中的两个应用

```
应用 1：Controller 选主（DLedger）
  → 管理 Broker 主从切换
  → 保证选主决策的一致性
  → Controller 集群自身高可用

应用 2：CommitLog 多副本（DLedger Commitlog，可选）
  → CommitLog 基于 Raft 多副本复制
  → 多数派确认后才返回成功
  → 自动选主，零数据丢失
```

## 📐 Raft 核心机制详解

### 1. Leader 选举

```
① Follower 超时未收到心跳（随机 150-300ms）
   │
   ▼
② Follower 变为 Candidate，Term+1
   │
   ▼
③ Candidate 投票给自己，向其他节点请求投票
   │
   ▼
④ 其他节点投票（每个 Term 只投一票）
   │
   ├─ 获得多数派投票 → 成为 Leader
   └─ 收到更高 Term 的 Leader 心跳 → 变回 Follower
```

### 2. 日志复制

```
① Leader 接收写请求
   │
   ▼
② Leader 追加日志到本地
   │
   ▼
③ Leader 并行复制给所有 Follower
   │
   ▼
④ 多数派确认（包括 Leader 自己）
   │
   ▼
⑤ Leader 提交（Commit）
   │
   ▼
⑥ Leader 应用到状态机
   │
   ▼
⑦ Leader 通知 Follower 提交
   │
   ▼
⑧ Follower 应用到状态机
```

### 3. 安全性保证

```
选举限制：
  Candidate 必须包含所有已提交的日志才能当选
  → 保证新 Leader 一定有最新数据

提交规则：
  Leader 只能提交当前 Term 的日志
  → 防止旧 Term 的日志被覆盖

日志匹配：
  如果两个节点在某个位置的日志相同
  → 该位置之前的所有日志都相同
```

## 📊 DLedger vs JRaft vs ZooKeeper

| 维度 | DLedger | JRaft | ZooKeeper |
|------|---------|-------|-----------|
| 语言 | Java | Java | Java |
| 算法 | Raft | Raft | ZAB |
| 用途 | Controller + CommitLog | 通用 Raft | 协调服务 |
| 性能 | 高 | 高 | 中 |
| 复杂度 | 低 | 中 | 高 |
| RocketMQ | ✅ 默认 | 可选 | 不用 |

## 💡 设计亮点

### 1. 事件驱动架构

```
Controller 的所有操作都封装为 Event：
  - 写事件 → DLedger 共识 → 应用到状态机
  - 读事件 → 直接读取状态机

好处：
  - 写操作保证一致性
  - 读操作高性能
  - 事件可重放、可恢复
```

### 2. 状态机分离

```
DLedgerControllerStateMachine：
  - 接收已提交的日志
  - 反序列化为 EventMessage
  - 应用到 ReplicasInfoManager

好处：
  - 状态机逻辑与 Raft 共识分离
  - 便于测试和维护
```

### 3. 两阶段选主

```
Controller 选主（两阶段）：

阶段 1：Controller 选出新 Master
  → 通过 Raft 共识保证选主决策一致

阶段 2：通知 Broker 切换角色
  → 新 Master changeToMaster()
  → 其他 Slave changeToSlave()

好处：
  - 选主决策一致（Raft 保证）
  - 角色切换可控（通知机制）
```

---

## 💡 学习收获

1. **Raft 核心**：Leader 选举 + 日志复制 + 安全性
2. **DLedger**：Java 实现的 Raft，用于 Controller 和可选的 CommitLog
3. **Controller 模式**：通过 Raft 共识管理 Broker 主从切换
4. **事件驱动**：写操作通过 Raft 共识，读操作直接本地读
5. **两阶段选主**：Controller 选主 + Broker 角色切换
