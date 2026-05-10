# ProxyStartup — Proxy 启动入口

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`
> 🏷️ #核心类 #启动流程 #架构设计

## 🎯 类的职责

`ProxyStartup` 是 RocketMQ Proxy 的 **唯一入口点**。它负责：
1. 解析命令行参数
2. 初始化配置
3. 创建消息处理器（MessagingProcessor）
4. 构建 gRPC 服务器
5. 构建 Remoting 服务器（兼容 4.x 协议）
6. 按顺序启动所有组件
7. 注册 ShutdownHook 优雅关闭

## 📐 整体架构图

```
┌─────────────────────────────────────────────────┐
│                  ProxyStartup.main()             │
│                                                   │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ 命令行解析 │→│ 配置初始化 │→│ 线程池监控初始化 │  │
│  └──────────┘  └──────────┘  └───────────────┘  │
│                                                   │
│  ┌──────────────────────────────────────────┐    │
│  │         createMessagingProcessor()        │    │
│  │  ┌─────────────┐   ┌─────────────────┐  │    │
│  │  │ ClusterMode  │   │   LocalMode     │  │    │
│  │  │ (纯Proxy)    │   │ (内嵌Broker)    │  │    │
│  │  └──────┬──────┘   └───────┬─────────┘  │    │
│  └─────────┼──────────────────┼─────────────┘    │
│            ▼                  ▼                    │
│  ┌──────────────┐   ┌────────────────┐           │
│  │  GrpcServer  │   │ RemotingServer │           │
│  │  (gRPC协议)   │   │ (4.x兼容协议)  │           │
│  └──────────────┘   └────────────────┘           │
│                                                   │
│  ┌─────────────────────────────────────────┐     │
│  │     PROXY_START_AND_SHUTDOWN.start()     │     │
│  │         (按注册顺序依次启动)              │     │
│  └─────────────────────────────────────────┘     │
└─────────────────────────────────────────────────┘
```

## 🔍 逐段解析

### 1. 启动编排 — `main()` 方法

```java
public static void main(String[] args) {
    // ① 解析命令行参数
    CommandLineArgument commandLineArgument = parseCommandLineArgument(args);
    initConfiguration(commandLineArgument);

    // ② 初始化线程池监控（JStack 打印、线程池状态上报）
    initThreadPoolExecutor();

    // ③ 创建 gRPC 服务端线程池
    ThreadPoolExecutor executor = createServerExecutor();

    // ④ 创建消息处理器（核心！根据模式选择不同实现）
    MessagingProcessor messagingProcessor = createMessagingProcessor();

    // ⑤ TLS 证书管理器（支持热更新）
    TlsCertificateManager tlsCertificateManager = new TlsCertificateManager();

    // ⑥ 构建 gRPC 服务器（Builder 模式）
    GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor, port, tlsCertificateManager)
        .addService(createServiceProcessor(messagingProcessor))  // 注册业务服务
        .addService(ChannelzService.newInstance(100))             // gRPC 通道诊断
        .addService(ProtoReflectionService.newInstance())         // Protobuf 反射
        .configInterceptor()                                      // 注册拦截器链
        .shutdownTime(...)                                        // 优雅关闭超时
        .build();

    // ⑦ 构建 Remoting 服务器（兼容 RocketMQ 4.x 客户端）
    RemotingProtocolServer remotingServer = new RemotingProtocolServer(...);

    // ⑧ 按注册顺序依次启动
    PROXY_START_AND_SHUTDOWN.start();

    // ⑨ 注册 JVM ShutdownHook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        PROXY_START_AND_SHUTDOWN.preShutdown();
        PROXY_START_AND_SHUTDOWN.shutdown();
    }));
}
```

> [!tip] 设计亮点
> 使用 `PROXY_START_AND_SHUTDOWN`（`AbstractStartAndShutdown`）统一管理所有组件的生命周期。
> 组件按 `appendStartAndShutdown()` 的注册顺序**顺序启动、逆序关闭**，保证依赖关系正确。

---

### 2. 双模式设计 — `createMessagingProcessor()`

```java
protected static MessagingProcessor createMessagingProcessor() {
    String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();

    if (ProxyMode.isClusterMode(proxyModeStr)) {
        // 集群模式：纯 Proxy，连接外部 Broker
        messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
    } else if (ProxyMode.isLocalMode(proxyModeStr)) {
        // 本地模式：内嵌 Broker（开发/测试用）
        BrokerController brokerController = createBrokerController();
        messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController);
    }
}
```

> [!important] 两种运行模式
> | | **Local 模式** | **Cluster 模式** |
> |---|---|---|
> | Broker | 内嵌启动 | 外部独立部署 |
> | 适用场景 | 开发、测试、单机部署 | 生产环境、大规模集群 |
> | 架构 | `Client → Proxy(+Broker)` | `Client → Proxy → Broker` |
> | 配置 | 需要 `brokerConfigPath` | 只需 `namesrvAddr` |

> [!note] #设计模式 — 策略模式
> `MessagingProcessor` 是一个接口，`DefaultMessagingProcessor` 根据模式选择不同的创建方式。
> 这是典型的**策略模式**：同一接口，不同运行时行为。

---

### 3. gRPC 服务器构建

```java
GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor, port, tlsCertificateManager)
    .addService(createServiceProcessor(messagingProcessor))  // ① 注册 gRPC 服务
    .addService(ChannelzService.newInstance(100))             // ② 通道诊断
    .addService(ProtoReflectionService.newInstance())         // ③ 反射服务
    .configInterceptor()                                      // ④ 拦截器链
    .shutdownTime(...)                                        // ⑤ 优雅关闭
    .build();
```

> [!note] gRPC 辅助服务
> - `ChannelzService`：gRPC 内置的通道诊断服务，可以查看连接状态、RPC 统计等
> - `ProtoReflectionService`：Protobuf 反射服务，允许 gRPCurl 等工具动态查询接口定义
> - 这两个在**生产调试**时非常有用

---

### 4. 生命周期管理 — `AbstractStartAndShutdown`

```java
private static class ProxyStartAndShutdown extends AbstractStartAndShutdown {
    @Override
    public void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
        super.appendStartAndShutdown(startAndShutdown);
    }
}
```

**启动顺序**（按注册顺序）：
1. `TlsCertificateManager` — TLS 证书
2. `GrpcServer` — gRPC 服务
3. `RemotingProtocolServer` — 4.x 兼容服务
4. `BrokerController`（Local 模式）/ `ProxyMetricsManager`（Cluster 模式）
5. `MessagingProcessor` — 消息处理器

**关闭顺序**：逆序关闭（LIFO），保证依赖不被提前销毁。

> [!tip] #源码技巧
> `AbstractStartAndShutdown` 是一个通用的生命周期管理器，内部用 `List<StartAndShutdown>` 保存组件。
> - `start()` → 正序遍历调用 `start()`
> - `shutdown()` → 逆序遍历调用 `shutdown()`
> 这个模式可以复用到任何需要管理多组件生命周期的系统中。

---

### 5. ShutdownHook — 优雅关闭

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    log.info("try to shutdown server");
    try {
        PROXY_START_AND_SHUTDOWN.preShutdown();  // 预关闭（通知客户端）
        PROXY_START_AND_SHUTDOWN.shutdown();      // 实际关闭
    } catch (Exception e) {
        log.error("err when shutdown rocketmq-proxy", e);
    }
}));
```

> [!note] 两阶段关闭
> `preShutdown()` + `shutdown()` 是两阶段关闭设计：
> - `preShutdown()`：通知连接的客户端"我要关了"，让它们有时间迁移
> - `shutdown()`：真正释放资源
> 这种设计在 gRPC 的 `GracefulShutdown` 中也有体现。

---

### 6. 线程池设计

```java
public static ThreadPoolExecutor createServerExecutor() {
    ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
        threadPoolNums,        // 核心线程数 = 最大线程数
        threadPoolNums,
        1, TimeUnit.MINUTES,   // 空闲线程 1 分钟回收
        "GrpcRequestExecutorThread",
        threadPoolQueueCapacity // 有界队列
    );
}
```

> [!important] 线程池隔离
> `GrpcMessagingApplication` 中为不同类型的请求创建了**独立线程池**：
> - `GrpcRouteThreadPool` — 路由查询
> - `GrpcProducerThreadPool` — 生产者请求
> - `GrpcConsumerThreadPool` — 消费者请求
> - `GrpcClientManagerThreadPool` — 客户端管理
> - `GrpcTransactionThreadPool` — 事务消息
>
> **好处**：某类请求打满不会影响其他类型的请求。这是**线程池隔离**的经典实践。

---

## 🧩 关键设计模式总结

| 设计模式 | 在哪里 | 作用 |
|----------|--------|------|
| **Builder 模式** | `GrpcServerBuilder` | 灵活构建 gRPC 服务器 |
| **策略模式** | `MessagingProcessor` | Local/Cluster 双模式 |
| **责任链模式** | `RequestPipeline` | 请求预处理（认证、鉴权、上下文初始化） |
| **生命周期管理** | `AbstractStartAndShutdown` | 统一组件启停顺序 |
| **线程池隔离** | `GrpcMessagingApplication` | 不同请求类型互不影响 |
| **优雅关闭** | `preShutdown + shutdown` | 两阶段安全关闭 |

---

## 💡 学习收获

1. **入口类应该做什么**：只做编排（orchestration），不包含业务逻辑
2. **生命周期管理**：用统一的 Start/Shutdown 接口管理所有组件
3. **双模式架构**：通过策略模式支持"单机"和"集群"两种部署形态
4. **线程池隔离**：不同功能使用独立线程池，避免互相影响
5. **优雅关闭**：两阶段关闭保证连接不丢失

---

> ⏭️ 下一篇：[[02-ProxyMode]] — Local/Cluster 双模式的详细设计
