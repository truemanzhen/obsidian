# NameServer 路由 — 无状态的路由注册中心

> 📄 源码路径：`namesrv/src/main/java/org/apache/rocketmq/namesrv/`
> 🏷️ #核心类 #架构设计 #面试高频

## 🎯 核心思想

NameServer 是 RocketMQ 的**路由注册中心**，负责：
1. Broker 注册（Broker 启动时注册自己的地址和 Topic 信息）
2. 路由发现（Producer/Consumer 查询 Topic 路由）
3. 心跳检测（定期检测 Broker 是否存活）

**关键特性：无状态、多节点对等、不互相通信。**

## 📐 架构设计

```
┌──────────────────────────────────────────────────────────────────┐
│                        NameServer 集群                            │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ NameServer-1 │  │ NameServer-2 │  │ NameServer-3 │           │
│  │              │  │              │  │              │           │
│  │ RouteInfo    │  │ RouteInfo    │  │ RouteInfo    │           │
│  │ Manager      │  │ Manager      │  │ Manager      │           │
│  │ (内存路由表)  │  │ (内存路由表)  │  │ (内存路由表)  │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│         ↑ 不互相通信 ↑                                            │
└──────────────────────────────────────────────────────────────────┘
         ↑                    ↑                    ↑
         │                    │                    │
    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
    │ Broker  │          │Producer │          │Consumer │
    │ (注册)   │          │(查询路由)│          │(查询路由)│
    └─────────┘          └─────────┘          └─────────┘
```

> [!important] NameServer 的核心设计
> - **无状态**：不持久化路由数据，全部在内存中
> - **多节点对等**：各 NameServer 之间不通信，数据独立
> - **最终一致**：Broker 向所有 NameServer 注册，最终各节点数据一致
> - **简单可靠**：没有选举、没有主从、没有分布式协调

## 📐 核心数据结构 — RouteInfoManager

```java
public class RouteInfoManager {
    // Topic 路由表：Topic → Queue 列表
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;

    // Broker 地址表：BrokerName → (BrokerId → BrokerAddr)
    private final HashMap<String/* brokerName */, HashMap<Long/* brokerId */, String>> brokerAddrTable;

    // Broker 集群表：ClusterName → BrokerName 集合
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    // Broker 存活表：BrokerAddr → 存活信息（最后心跳时间等）
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    // Broker 过滤服务器表
    private final HashMap<String/* brokerAddr */, List<String>> filterServerTable;
}
```

> [!tip] 五张表的含义
> | 表 | Key | Value | 用途 |
> |---|---|---|---|
> | `topicQueueTable` | Topic | QueueData 列表 | Producer 查询路由 |
> | `brokerAddrTable` | BrokerName | BrokerId→Addr | 主从地址发现 |
> | `clusterAddrTable` | ClusterName | BrokerName 集合 | 集群管理 |
> | `brokerLiveTable` | BrokerAddr | BrokerLiveInfo | 心跳检测 |
> | `filterServerTable` | BrokerAddr | FilterServer 列表 | 消息过滤 |

## 🔍 核心流程

### 1. Broker 注册 — registerBroker()

```java
public RegisterBrokerResult registerBroker(String clusterName, String brokerAddr,
    String brokerName, long brokerId, String haServerAddr, ...) {

    // ① 更新集群表
    Set<String> brokerNames = clusterAddrTable.get(clusterName);
    brokerNames.add(brokerName);

    // ② 更新 Broker 地址表
    HashMap<Long, String> brokerAddrs = brokerAddrTable.get(brokerName);
    brokerAddrs.put(brokerId, brokerAddr);

    // ③ 更新 Topic 路由表
    if (topicConfigWrapper != null) {
        for (Map.Entry<String, TopicConfig> entry : topicConfigWrapper.getTopicConfigTable().entrySet()) {
            TopicConfig topicConfig = entry.getValue();
            // 构建 QueueData
            QueueData queueData = new QueueData(topicConfig.getWriteQueueNums(),
                topicConfig.getReadQueueNums(), topicConfig.getPerm());
            topicQueueTable.get(topicConfig.getTopicName()).add(queueData);
        }
    }

    // ④ 更新存活表
    brokerLiveTable.put(new BrokerAddrInfo(clusterName, brokerAddr),
        new BrokerLiveInfo(System.currentTimeMillis(), haServerAddr, ...));

    // ⑤ 如果是 Slave，返回 Master 地址
    if (brokerId == MixAll.MASTER_ID) {
        // 更新 Master 地址
    } else {
        // 返回 Master 的 HA 地址
        result.setHaServerAddr(masterAddr);
    }
}
```

### 2. 路由查询 — DefaultRequestProcessor

```java
// Producer/Consumer 查询路由
case RequestCode.GET_ROUTEINFO_BY_TOPIC:
    return this.getRouteInfoByTopic(ctx, request);

public RemotingCommand getRouteInfoByTopic(...) {
    // ① 从 topicQueueTable 获取 Queue 列表
    List<QueueData> queueDataList = routeInfoManager.topicQueueTable.get(topic);

    // ② 从 brokerAddrTable 获取 Broker 地址
    for (QueueData qd : queueDataList) {
        HashMap<Long, String> brokerAddrs = routeInfoManager.brokerAddrTable.get(qd.getBrokerName());
        brokerDataList.add(new BrokerData(clusterName, qd.getBrokerName(), brokerAddrs));
    }

    // ③ 构建 TopicRouteData 返回
    TopicRouteData topicRouteData = new TopicRouteData(queueDataList, brokerDataList);
}
```

### 3. 心跳检测 — BrokerHousekeepingService

```java
// 定时扫描不活跃的 Broker（默认 120 秒超时）
public void scanNotActiveBroker() {
    for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry : brokerLiveTable.entrySet()) {
        long last = entry.getValue().getLastUpdateTimestamp();
        long timeout = entry.getValue().getHeartbeatTimeoutMillis();

        if ((System.currentTimeMillis() - last) > timeout) {
            // Broker 超时，移除路由信息
            onBrokerChannelDestroy(entry.getKey());
        }
    }
}
```

## 📐 TopicRouteData 结构

```java
public class TopicRouteData {
    private List<QueueData> queueDataList;    // Queue 列表
    private List<BrokerData> brokerDataList;  // Broker 地址列表
}

public class QueueData {
    private String brokerName;      // Broker 名称
    private int readQueueNums;      // 读 Queue 数量
    private int writeQueueNums;     // 写 Queue 数量
    private int perm;               // 权限（读/写）
    private int topicSynFlag;       // 同步标志
}

public class BrokerData {
    private String cluster;                    // 集群名
    private String brokerName;                 // Broker 名
    private HashMap<Long, String> brokerAddrs; // BrokerId → 地址
}
```

## 📐 客户端路由发现流程

```
Producer/Consumer 启动时：

① 从 NameServer 列表中随机选一个
   namesrvAddr = "192.168.1.1:9876;192.168.1.2:9876"

② 定时获取路由（默认 30 秒）
   GET_ROUTEINFO_BY_TOPIC → topicRouteData

③ 本地缓存路由
   topicRouteTable.put(topic, topicRouteData)

④ 路由变化时更新
   NameServer 返回的路由与本地不一致 → 更新本地缓存

⑤ Broker 宕机检测
   NameServer 检测到 Broker 客户端 → 移除路由 → 客户端下次获取时更新
```

## 💡 设计亮点

### 1. 极简设计

```
NameServer 不做：
  - 不持久化数据（全内存）
  - 不互相通信（无集群协调）
  - 不选举（无主从）
  - 不推路由（客户端主动拉）

NameServer 只做：
  - 接收 Broker 注册
  - 返回路由信息
  - 心跳检测
```

### 2. 最终一致性

```
Broker 启动 → 向所有 NameServer 注册
  → NameServer-1 有路由
  → NameServer-2 有路由
  → NameServer-3 有路由（可能有延迟）

Producer 从任意 NameServer 获取路由 → 最终一致
```

### 3. 高可用

```
任何一个 NameServer 宕机：
  - 其他 NameServer 仍然有完整路由
  - Producer/Consumer 自动切换到其他 NameServer
  - 系统不受影响

所有 NameServer 宕机：
  - Producer/Consumer 使用本地缓存的路由
  - 仍可正常收发消息（直到路由变化）
```

---

## 💡 学习收获

1. **无状态设计**：全内存、不持久化、不互相通信，极简可靠
2. **五张路由表**：topicQueueTable + brokerAddrTable + clusterAddrTable + brokerLiveTable + filterServerTable
3. **拉模式**：客户端主动拉取路由，不推送
4. **最终一致**：Broker 向所有 NameServer 注册，客户端从任意一个获取
5. **高可用**：任何 NameServer 宕机不影响系统
