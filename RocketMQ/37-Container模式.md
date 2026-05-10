# Container 模式

> 源码位置：`container/src/main/java/org/apache/rocketmq/container/`

## 🏗️ 什么是 Container 模式

Container（容器）模式是 5.x 引入的**多 Broker 实例共享进程**的设计，允许在一个 JVM 进程中运行多个独立的 Broker。

```
传统模式：
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Broker-A │  │ Broker-B │  │ Broker-C │  (3个独立进程)
│ JVM 1    │  │ JVM 2    │  │ JVM 3    │
└──────────┘  └──────────┘  └──────────┘

Container 模式：
┌──────────────────────────────────────────┐
│            BrokerContainer               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐│
│  │ Broker-A │ │ Broker-B │ │ Broker-C ││  (1个进程)
│  │ (Inner)  │ │ (Inner)  │ │ (Inner)  ││
│  └──────────┘ └──────────┘ └──────────┘│
│         共享 RemotingServer              │
└──────────────────────────────────────────┘
```

## 📊 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                   BrokerContainer                         │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │ masterBrokerControllers (Master Broker 实例集)      │ │
│  │ slaveBrokerControllers  (Slave Broker 实例集)       │ │
│  │ dLedgerBrokerControllers (DLedger Broker 实例集)    │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │ RemotingServer (共享网络层)                          │ │
│  │   └── 多个 InnerBrokerController 共享同一个 Server   │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │ BrokerContainerProcessor (Container 级处理器)       │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `BrokerContainer` | Container 入口，管理多个 Broker 实例 |
| `IBrokerContainer` | Container 接口 |
| `InnerBrokerController` | Container 内的 Broker 实例（Master） |
| `InnerSalveBrokerController` | Container 内的 Slave 实例 |
| `BrokerContainerProcessor` | Container 级别的请求处理器 |
| `BrokerContainerConfig` | Container 配置 |
| `BrokerContainerStartup` | 启动入口 |
| `ContainerClientHouseKeepingService` | 客户端连接管理 |

## 🔄 核心流程

### 启动流程

```
BrokerContainerStartup.main(args)
    ↓
创建 BrokerContainer
    ├── 初始化 NettyServerConfig
    ├── 初始化 NettyClientConfig
    ├── 创建 BrokerOuterAPI
    └── 创建 BrokerContainerProcessor
    ↓
添加 InnerBrokerController
    ├── brokerContainer.addMasterBroker(new InnerBrokerController(...))
    ├── brokerContainer.addMasterBroker(new InnerBrokerController(...))
    └── ...
    ↓
brokerContainer.start()
    ├── 启动 RemotingServer（共享）
    ├── 启动所有 InnerBrokerController
    │   ├── 启动 MessageStore
    │   ├── 启动定时任务
    │   └── 注册到 NameServer
    └── 启动 BrokerContainerProcessor
```

### 请求路由

```
请求到达 BrokerContainer RemotingServer
    ↓
BrokerContainerProcessor.processRequest(ctx, request)
    ├── 解析请求中的 BrokerIdentity
    ├── 路由到对应的 InnerBrokerController
    └── InnerBrokerController 的 Processor 处理请求
```

## 🔧 InnerBrokerController

```java
public class InnerBrokerController extends BrokerController {
    protected BrokerContainer brokerContainer;

    @Override
    protected void initializeRemotingServer() {
        // 不创建自己的 RemotingServer，使用 Container 的
        RemotingServer remotingServer = this.brokerContainer.getRemotingServer()
            .newRemotingServer(brokerConfig.getListenPort());
        this.setRemotingServer(remotingServer);
    }

    @Override
    public BrokerOuterAPI getBrokerOuterAPI() {
        // 使用 Container 的 BrokerOuterAPI（共享连接池）
        return this.brokerContainer.getBrokerOuterAPI();
    }
}
```

**关键设计：**
- 共享 RemotingServer（减少端口占用）
- 共享 BrokerOuterAPI（减少连接数）
- 独立 MessageStore（数据隔离）
- 独立定时任务（逻辑隔离）

## 📋 配置示例

```java
BrokerContainerConfig containerConfig = new BrokerContainerConfig();
containerConfig.setBrokerContainerName("Container-1");

// 创建多个 Broker 配置
BrokerConfig brokerAConfig = new BrokerConfig();
brokerAConfig.setBrokerName("Broker-A");
brokerAConfig.setListenPort(10911);

BrokerConfig brokerBConfig = new BrokerConfig();
brokerBConfig.setBrokerName("Broker-B");
brokerBConfig.setListenPort(10912);

// 添加到 Container
BrokerContainer container = new BrokerContainer(containerConfig, nettyServerConfig, nettyClientConfig);
container.addMasterBroker(new InnerBrokerController(container, brokerAConfig, storeConfig, authConfig));
container.addMasterBroker(new InnerBrokerController(container, brokerBConfig, storeConfig, authConfig));
container.start();
```

## 💡 设计亮点

1. **资源复用**：共享 RemotingServer、连接池、线程池
2. **数据隔离**：每个 Broker 有独立的 MessageStore
3. **统一管理**：Container 统一管理生命周期
4. **端口复用**：多个 Broker 可以共享端口（通过 BrokerIdentity 路由）
5. **平滑迁移**：从独立 Broker 迁移到 Container 模式

## ⚠️ 注意事项

1. **故障隔离**：一个 Broker 异常可能影响整个 Container
2. **GC 压力**：多个 Broker 共享 JVM，GC 停顿影响更大
3. **内存管理**：需要合理分配每个 Broker 的内存
4. **调试复杂度**：问题定位更困难

## 🔗 与其他模块的关系

- **Broker**：InnerBrokerController 继承自 BrokerController
- **Remoting**：共享 NettyRemotingServer
- **Store**：每个 InnerBrokerController 有独立的 MessageStore
- **NameServer**：每个 InnerBrokerController 独立注册

---

#设计模式 #源码技巧
