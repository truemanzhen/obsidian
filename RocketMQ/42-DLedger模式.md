# DLedger 模式

> 源码位置：`broker/dledger/` + `store/dledger/` + `controller/`

## 🏗️ 什么是 DLedger

DLedger 是 RocketMQ 自研的**基于 Raft 协议的分布式日志存储**，用于实现 Broker 的**高可用**和**自动选主**。

```
传统主从模式：
┌─────────┐     同步复制     ┌─────────┐
│  Master │ ──────────────→ │  Slave  │
│ Broker  │                 │ Broker  │
└─────────┘                 └─────────┘
问题：Master 宕机需要手动切换

DLedger 模式：
┌─────────┐  ┌─────────┐  ┌─────────┐
│ DLedger │  │ DLedger │  │ DLedger │
│ Node 1  │  │ Node 2  │  │ Node 3  │
│(Leader) │  │(Follower)│ │(Follower)│
└─────────┘  └─────────┘  └─────────┘
     ↑ Raft 共识协议，自动选主
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                   BrokerController                        │
├─────────────────────────────────────────────────────────┤
│                DefaultMessageStore                       │
│  ┌──────────────────────────────────────────────────┐  │
│  │              DLedgerCommitLog                     │  │
│  │  ┌─────────────┬─────────────┬──────────────┐   │  │
│  │  │ DLedgerServer│ DLedgerStore│ RaftLog      │   │  │
│  │  │ (Raft节点)   │ (日志存储)  │ (Raft日志)   │   │  │
│  │  └─────────────┴─────────────┴──────────────┘   │  │
│  └──────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────┤
│              DLedgerRoleChangeHandler                    │
│         (角色变化时更新 Broker 状态)                      │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `DLedgerCommitLog` | 基于 DLedger 的 CommitLog 实现 |
| `DLedgerRoleChangeHandler` | 角色变化处理（Leader/Follower 切换） |
| `DLedgerServer` | DLedger 服务端（Raft 节点） |
| `DLedgerStore` | DLedger 存储层 |
| `MemberState` | 成员状态（当前角色、Leader 地址等） |

## 🔄 角色变化处理

### DLedgerRoleChangeHandler

```java
public class DLedgerRoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

    @Override
    public void handle(long term, MemberState.Role role) {
        switch (role) {
            case LEADER:
                // 成为 Leader
                handleBecomeLeader(term);
                break;
            case FOLLOWER:
                // 成为 Follower
                handleBecomeFollower(term);
                break;
            case CANDIDATE:
                // 成为 Candidate（选举中）
                handleBecomeCandidate(term);
                break;
        }
    }

    private void handleBecomeLeader(long term) {
        // 1. 更新 Broker 角色为 MASTER
        messageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);

        // 2. 更新 CommitLog 的写权限
        dLedgerCommitLog.setWritable(true);

        // 3. 注册到 NameServer
        brokerController.registerBrokerAll();

        // 4. 启动定时任务
        brokerController.startScheduleTasks();
    }

    private void handleBecomeFollower(long term) {
        // 1. 更新 Broker 角色为 SLAVE
        messageStoreConfig.setBrokerRole(BrokerRole.SLAVE);

        // 2. 关闭写权限
        dLedgerCommitLog.setWritable(false);

        // 3. 取消注册
        brokerController.unregisterBrokerAll();
    }
}
```

## 📝 DLedgerCommitLog

### 写入流程

```java
public class DLedgerCommitLog extends CommitLog {

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // 1. 编码消息
        byte[] body = encode(msg);

        // 2. 构建 DLedger 请求
        DLedgerRequest request = new DLedgerRequest();
        request.setBody(body);

        // 3. 提交到 DLedger（Raft 共识）
        CompletableFuture<DLedgerResponse> future =
            dLedgerServer.appendAsLeader(request);

        // 4. 等待多数节点确认
        DLedgerResponse response = future.get(timeout, TimeUnit.MILLISECONDS);

        // 5. 返回结果
        if (response.getCode() == DLedgerResponseCode.SUCCESS) {
            return new PutMessageResult(PutMessageStatus.PUT_OK, ...);
        } else {
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, ...);
        }
    }
}
```

### Raft 日志复制

```
Leader 收到写请求
    ↓
1. 写入本地 Raft Log
    ↓
2. 并行发送给所有 Follower
    ↓
3. 等待多数节点（N/2 + 1）确认
    ↓
4. 提交（Commit）
    ↓
5. 应用到状态机（写入 CommitLog）
```

## 🗳️ 选主机制

### Raft 选举流程

```
1. 所有节点初始为 Follower
    ↓
2. Follower 超时未收到 Leader 心跳
    ↓
3. 转为 Candidate，发起投票
    ├── 递增 term（任期）
    ├── 投票给自己
    └── 向其他节点发送 RequestVote
    ↓
4. 收到多数票 → 成为 Leader
   收到 Leader 心跳 → 回退为 Follower
   选举超时 → 重新选举
```

### term（任期）

```java
// 每次选举递增 term
// 用于区分不同任期的 Leader
long term = memberState.currTerm();

// Leader 写入时携带 term
// Follower 检查 term 是否最新
```

## 📊 三种部署模式

### 1. DLedger 模式（自动选主）

```properties
# 启用 DLedger
enableDLegerCommitLog=true
dLegerGroup=RaftGroup-001
dLegerPeers=n0-192.168.1.1:40911;n1-192.168.1.2:40911;n2-192.168.1.3:40911
dLegerSelfId=n0
```

### 2. Controller 模式（外部选主）

```properties
# 使用 Controller 选主
controllerAddr=192.168.1.100:9876
enableControllerMode=true
```

### 3. 传统主从模式

```properties
# 手动指定角色
brokerRole=SYNC_MASTER  # 或 ASYNC_MASTER, SLAVE
```

## 💡 设计亮点

1. **自动选主**：无需人工干预，Master 宕机自动切换
2. **Raft 共识**：多数节点确认才返回成功，保证数据不丢失
3. **角色透明**：CommitLog 接口不变，上层无感知
4. **term 机制**：防止脑裂，确保只有一个 Leader
5. **优雅降级**：DLedger 故障时可以回退到传统模式

## ⚠️ 注意事项

1. **奇数节点**：建议 3 或 5 个节点（2N+1）
2. **网络延迟**：Raft 共识需要多数节点确认，网络延迟影响写入性能
3. **存储开销**：Raft Log + CommitLog 双份存储
4. **切换延迟**：选主过程需要几秒到十几秒
5. **版本兼容**：DLedger 模式和传统模式不兼容，不能混合部署

## 🔗 与其他模块的关系

- **CommitLog**：DLedgerCommitLog 继承 CommitLog，替换底层存储
- **DefaultMessageStore**：通过配置切换到 DLedgerCommitLog
- **Controller**：Controller 模式是 DLedger 的升级版，解耦选主和存储
- **HA**：DLedger 模式替代了传统的 HA 主从同步

---

#核心类 #设计模式 #面试高频
