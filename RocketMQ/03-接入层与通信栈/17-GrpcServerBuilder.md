# GrpcServerBuilder - gRPC 服务器构建器

> 研究定位：只看 gRPC Server 怎么被组装，哪些能力属于构建阶段，哪些能力属于生命周期阶段。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServerBuilder.java`、`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/ProxyAndTlsProtocolNegotiator.java`

## 先给结论

- `GrpcServerBuilder` 只负责“搭建”，不负责“运行”。
- 它把 Netty、TLS、Proxy Protocol、线程模型和拦截器统一封装起来。
- 真正的启动/关闭在 `GrpcServer`。
- `configInterceptor()` 注册的是业务层前置处理，不是网络层协议解析。

## 源码骨架

```java
serverBuilder = NettyServerBuilder.forPort(port)
    .maxConcurrentCallsPerConnection(config.getGrpcMaxConcurrentCallsPerConnection());
serverBuilder.protocolNegotiator(new ProxyAndTlsProtocolNegotiator());
if (config.isEnableGrpcEpoll()) {
    serverBuilder.bossEventLoopGroup(new EpollEventLoopGroup(bossLoopNum))
        .workerEventLoopGroup(new EpollEventLoopGroup(workerLoopNum))
        .channelType(EpollServerSocketChannel.class)
        .executor(executor);
} else {
    serverBuilder.bossEventLoopGroup(new NioEventLoopGroup(bossLoopNum))
        .workerEventLoopGroup(new NioEventLoopGroup(workerLoopNum))
        .channelType(NioServerSocketChannel.class)
        .executor(executor);
}
```

## 构建主线

### 1. 并发上限

`maxConcurrentCallsPerConnection` 是单连接并发上限，用来避免单个客户端把某个 Proxy 打成热点。

### 2. 协议协商

`ProxyAndTlsProtocolNegotiator` 同时处理：

- TLS
- 明文
- Proxy Protocol
- `CHANNEL_ID` 注入

这一步是连接层，不是业务层。

### 3. 事件循环模型

- `enableGrpcEpoll=true` 时走 Linux epoll
- 否则走 NIO

`executor(executor)` 则是把业务请求派发到 Proxy 自己的线程池。

### 4. 消息和连接边界

`maxInboundMessageSize` 控制单条入站消息大小。

`maxConnectionIdle` 控制连接空闲超时。

## 拦截器注册

```java
public GrpcServerBuilder configInterceptor() {
    this.serverBuilder
        .intercept(new GlobalExceptionInterceptor())
        .intercept(new ContextInterceptor())
        .intercept(new HeaderInterceptor());
    return this;
}
```

gRPC 拦截器执行顺序和注册顺序相反，所以实际进入业务前的顺序是：

1. `HeaderInterceptor`
2. `ContextInterceptor`
3. `GlobalExceptionInterceptor`

## 这个构建器没有做什么

- 没有启动服务
- 没有注册 TLS 监听器
- 没有处理业务请求
- 没有决定消息怎么路由

这些都在后续对象里。

## 扩展知识

- `addService(BindableService)` 和 `addService(ServerServiceDefinition)` 说明它既能挂 generated service，也能挂手工定义的 service。
- `appendInterceptor()` 允许你在默认三层拦截器外再扩展别的拦截器。
- `GrpcServerBuilder` 和 `GrpcServer` 的分离，实际上就是 Builder/生命周期的标准拆分。

## 关联阅读

- [[18-GrpcServer]]

- [[19-GrpcMessagingApplication]]