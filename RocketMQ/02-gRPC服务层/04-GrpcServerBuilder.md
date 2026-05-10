# GrpcServerBuilder — Builder 模式构建 gRPC 服务器

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServerBuilder.java`
> 🏷️ #设计模式 #核心类

## 🎯 类的职责

使用 **Builder 模式** 构建和配置 gRPC 服务器，封装了 Netty 的底层细节。

## 🔍 源码逐行

### 构造器 — 服务器初始化

```java
protected GrpcServerBuilder(ThreadPoolExecutor executor, int port,
    TlsCertificateManager tlsCertificateManager) {

    ProxyConfig config = ConfigurationManager.getProxyConfig();

    // ① 创建 Netty 服务器构建器
    serverBuilder = NettyServerBuilder.forPort(port)
        .maxConcurrentCallsPerConnection(config.getGrpcMaxConcurrentCallsPerConnection());

    // ② 设置自定义协议协商器（支持 TLS + Proxy Protocol）
    serverBuilder.protocolNegotiator(new ProxyAndTlsProtocolNegotiator());

    // ③ 根据配置选择 Epoll（Linux）或 NIO（跨平台）
    if (config.isEnableGrpcEpoll()) {
        serverBuilder
            .bossEventLoopGroup(new EpollEventLoopGroup(bossLoopNum))    // Linux epoll
            .workerEventLoopGroup(new EpollEventLoopGroup(workerLoopNum))
            .channelType(EpollServerSocketChannel.class)
            .executor(executor);
    } else {
        serverBuilder
            .bossEventLoopGroup(new NioEventLoopGroup(bossLoopNum))      // 通用 NIO
            .workerEventLoopGroup(new NioEventLoopGroup(workerLoopNum))
            .channelType(NioServerSocketChannel.class)
            .executor(executor);
    }

    // ④ 消息大小限制 + 空闲连接超时
    serverBuilder
        .maxInboundMessageSize(maxInboundMessageSize)
        .maxConnectionIdle(idleTimeMills, TimeUnit.MILLISECONDS);
}
```

### 拦截器配置

```java
public GrpcServerBuilder configInterceptor() {
    this.serverBuilder
        .intercept(new GlobalExceptionInterceptor())  // ① 全局异常捕获（最外层）
        .intercept(new ContextInterceptor())           // ② gRPC Context 注入
        .intercept(new HeaderInterceptor());           // ③ 请求头解析（最内层）
    return this;
}
```

> [!important] 拦截器执行顺序
> gRPC 拦截器是**反序执行**的：后注册的先执行。
>
> ```
> 请求进入
>   → HeaderInterceptor     （解析远程地址、Channel ID）
>   → ContextInterceptor    （将 Metadata 注入 gRPC Context）
>   → GlobalExceptionInterceptor （全局异常兜底）
>   → 实际业务处理
> ```
>
> 但 `GlobalExceptionInterceptor` 包裹了整个调用链，所以它在最外层捕获异常。

## 📐 Netty 线程模型

```
┌─────────────────────────────────────────────┐
│              Netty Server                    │
│                                             │
│  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Boss Group   │  │   Worker Group       │  │
│  │ (accept 连接) │→│ (处理 I/O 读写)       │  │
│  └─────────────┘  └──────────┬──────────┘  │
│                              │              │
│                    ┌─────────▼──────────┐   │
│                    │  gRPC Executor      │   │
│                    │ (业务逻辑线程池)     │   │
│                    └────────────────────┘   │
└─────────────────────────────────────────────┘
```

> [!tip] 三层线程模型
> 1. **Boss Group**：负责 accept 新连接（通常 1 个线程）
> 2. **Worker Group**：负责 I/O 读写（CPU 核心数 * 2）
> 3. **gRPC Executor**：负责业务逻辑处理（可配置）
>
> 这种分层是 Netty 的标准做法，I/O 密集和 CPU 密集的任务用不同线程池处理。

## 💡 设计亮点

### 1. Epoll vs NIO 自动选择

```java
if (config.isEnableGrpcEpoll()) {
    // Linux: 用 epoll（性能更好，edge-triggered）
} else {
    // 跨平台: 用 NIO selector
}
```

> [!note] #源码技巧
> 在 Linux 上，epoll 比 NIO selector 性能更好（特别是大量连接时）。
> 通过配置开关自动选择，不需要改代码。

### 2. ProxyAndTlsProtocolNegotiator

自定义的协议协商器，支持：
- **TLS 终止**：在 Proxy 层完成 TLS 握手
- **Proxy Protocol**：支持 HAProxy 等负载均衡器传递真实客户端 IP
- **证书热更新**：通过 `TlsCertificateManager` 监听证书变更

---

## 🧩 关键配置项

| 配置项 | 作用 | 建议值 |
|--------|------|--------|
| `grpcBossLoopNum` | Boss 线程数 | 1 |
| `grpcWorkerLoopNum` | Worker 线程数 | CPU*2 |
| `grpcThreadPoolNums` | 业务线程数 | 根据 QPS 调整 |
| `grpcMaxInboundMessageSize` | 最大消息体 | 4MB |
| `grpcClientIdleTimeMills` | 空闲超时 | 120s |
| `enableGrpcEpoll` | 启用 epoll | Linux 下 true |

---

> ⏭️ 下一篇：[[05-GrpcServer]] — 服务器生命周期管理
