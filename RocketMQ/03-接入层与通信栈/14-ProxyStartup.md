# ProxyStartup - Proxy 启动入口

> 研究定位：只看启动编排，不把它误读成业务层。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`
> 关联测试：`proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java`

## 先给结论

- `ProxyStartup` 负责编排，不负责业务。
- 它把配置、线程池监控、消息处理器、gRPC 服务器、Remoting 服务器和关闭链串起来。
- `LOCAL` 和 `CLUSTER` 的差异，最终落在 `createMessagingProcessor()`。
- 启动顺序和关闭顺序都由 `AbstractStartAndShutdown` 决定，关闭是逆序。

## 源码骨架

```java
public static void main(String[] args) {
    CommandLineArgument cli = parseCommandLineArgument(args);
    initConfiguration(cli);
    initThreadPoolMonitor();
    ThreadPoolExecutor executor = createServerExecutor();
    MessagingProcessor processor = createMessagingProcessor();
    TlsCertificateManager tls = new TlsCertificateManager();
    GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor, port, tls)
        .addService(createServiceProcessor(processor))
        .configInterceptor()
        .shutdownTime(...)
        .build();
    RemotingProtocolServer remotingServer = new RemotingProtocolServer(processor, tls);
    PROXY_START_AND_SHUTDOWN.start();
}
```

## 启动主线

### 1. 解析命令行

支持的参数只有三类：

- `-bc`：`brokerConfigPath`
- `-pc`：`proxyConfigPath`
- `-pm`：`proxyMode`

还有一个来自公共参数的 `-n`，用于 `namesrvAddr`。

### 2. 初始化配置

`initConfiguration()` 的顺序是：

1. 如果传了 `proxyConfigPath`，先写入 `System` 属性。
2. `ConfigurationManager.initEnv()`
3. `ConfigurationManager.initConfig()`
4. 用命令行参数覆盖配置对象。
5. 打印格式化后的 Proxy 配置。

这里要注意，命令行覆盖的是配置对象，不是单独变量。

### 3. 初始化线程池监控

`initThreadPoolMonitor()` 不是业务逻辑，它只是在启动前把 JStack 打印和线程池状态监控挂上。

### 4. 创建业务线程池

`createServerExecutor()` 创建的是 gRPC 公共执行器，并把它的 `shutdown()` 注册进关闭链。

### 5. 创建消息处理器

`createMessagingProcessor()` 是模式分支点。

- `CLUSTER`：`DefaultMessagingProcessor.createForClusterMode()`
- `LOCAL`：先 `createBrokerController()`，再 `DefaultMessagingProcessor.createForLocalMode(brokerController)`

同时：

- `CLUSTER` 会初始化 `ProxyMetricsManager.initClusterMode(...)`
- `LOCAL` 会先初始化本地 Broker，再用包装对象加入关闭链

### 6. 创建 gRPC 服务

`createServiceProcessor()` 返回 `GrpcMessagingApplication.create(messagingProcessor)`，并把应用本身加入关闭链。

### 7. 创建两类服务器

- `GrpcServer`：gRPC 主入口
- `RemotingProtocolServer`：兼容 4.x 协议

这说明 Proxy 不是“只有 gRPC”，而是双协议入口。

### 8. 注册 ShutdownHook

关闭顺序是：

1. `preShutdown()`
2. `shutdown()`

这和 `AbstractStartAndShutdown` 的逆序关闭一致。

## 关键边界

- `ProxyStartup` 不做消息路由、不做消费确认、不做权限计算。
- 它只决定“先启动谁、后启动谁、怎么停”。
- `LOCAL` 模式下的 Broker 不是外部依赖，而是启动过程的一部分。

## 容易漏掉的点

- `ProxyMetricsManager` 并不是一个简单的工具类，`CLUSTER` 和 `LOCAL` 的初始化路径不同。
- `createBrokerController()` 会把 `namesrvAddr` 拼进 broker 启动参数。
- `parseCommandLineArgument()` 依赖 `ServerUtil`，不是自己手写 CLI 解析。

## 关联阅读

- [[15-ProxyMode]]
- [[16-ConfigurationManager]]
- [[17-GrpcServerBuilder]]
- [[18-GrpcServer]]
- [[19-GrpcMessagingApplication]]
