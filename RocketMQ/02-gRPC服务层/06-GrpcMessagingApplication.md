# GrpcMessagingApplication — gRPC 服务实现核心

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/GrpcMessagingApplication.java`
> 🏷️ #核心类 #面试高频 #线程池隔离

## 🎯 类的职责

这是 Proxy 的**核心类**，实现了 gRPC 的 `MessagingServiceGrpc.MessagingServiceImplBase`。
它负责：
1. 实现所有 gRPC 消息服务接口（sendMessage、receiveMessage、ackMessage 等）
2. **线程池隔离**：不同类型请求使用独立线程池
3. **请求管线**：通过 Pipeline 模式处理认证、鉴权、上下文初始化
4. **流量控制**：线程池满时返回 `TOO_MANY_REQUESTS`

## 📐 整体架构

```
gRPC 请求
    │
    ▼
┌──────────────────────────────────────────────────────┐
│           GrpcMessagingApplication                    │
│                                                      │
│  ┌────────────────────────────────────────────┐      │
│  │          RequestPipeline (责任链)            │      │
│  │                                            │      │
│  │  ContextInit → Authentication → Authorization│     │
│  └───────────────────┬────────────────────────┘      │
│                      ▼                               │
│  ┌────────────────────────────────────────────┐      │
│  │        线程池路由（按请求类型分发）           │      │
│  │                                            │      │
│  │  sendMessage    → ProducerThreadPool       │      │
│  │  receiveMessage → ConsumerThreadPool       │      │
│  │  queryRoute     → RouteThreadPool          │      │
│  │  heartbeat      → ClientManagerThreadPool  │      │
│  │  endTransaction → TransactionThreadPool    │      │
│  └───────────────────┬────────────────────────┘      │
│                      ▼                               │
│  ┌────────────────────────────────────────────┐      │
│  │     GrpcMessagingActivity (实际业务处理)     │      │
│  └────────────────────────────────────────────┘      │
└──────────────────────────────────────────────────────┘
```

## 🔍 逐段解析

### 1. 创建入口 — `create()` 静态方法

```java
public static GrpcMessagingApplication create(MessagingProcessor messagingProcessor) {
    // ① 创建空 Pipeline
    RequestPipeline pipeline = (context, headers, request) -> {};

    // ② 组装 Pipeline（注意顺序！后添加的先执行）
    AuthConfig authConfig = ConfigurationManager.getAuthConfig();
    if (authConfig != null) {
        pipeline = pipeline
            .pipe(new AuthorizationPipeline(authConfig, messagingProcessor))  // 鉴权
            .pipe(new AuthenticationPipeline(authConfig, messagingProcessor)); // 认证
    }
    pipeline = pipeline.pipe(new ContextInitPipeline());  // 上下文初始化

    // ③ 创建 Application
    return new GrpcMessagingApplication(
        new DefaultGrpcMessagingActivity(messagingProcessor), pipeline);
}
```

> [!important] Pipeline 的执行顺序
> 由于 `pipe()` 方法的实现（`source.execute() → execute()`），实际执行顺序是：
>
> ```
> ContextInitPipeline → AuthenticationPipeline → AuthorizationPipeline
> ```
>
> 即：**先初始化上下文，再认证，最后鉴权**。这是安全处理的标准顺序。

### 2. 线程池隔离 — 5 个独立线程池

```java
protected ThreadPoolExecutor routeThreadPoolExecutor;        // 路由查询
protected ThreadPoolExecutor producerThreadPoolExecutor;     // 生产者
protected ThreadPoolExecutor consumerThreadPoolExecutor;     // 消费者
protected ThreadPoolExecutor clientManagerThreadPoolExecutor; // 客户端管理
protected ThreadPoolExecutor transactionThreadPoolExecutor;   // 事务消息
```

**每个线程池的配置**（来自 `ProxyConfig`）：
```java
this.producerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
    config.getGrpcProducerThreadPoolNums(),      // 核心线程数
    config.getGrpcProducerThreadPoolNums(),      // 最大线程数
    1, TimeUnit.MINUTES,                         // 空闲回收
    "GrpcProducerThreadPool",                    // 线程名前缀
    config.getGrpcProducerThreadQueueCapacity()  // 队列容量
);
```

> [!tip] 为什么要线程池隔离？
> 假设所有请求共用一个线程池：
> - 如果消费者 `receiveMessage`（长轮询）占满线程 → 生产者 `sendMessage` 被阻塞
> - 如果路由查询变慢 → 所有请求都受影响
>
> 隔离后：消费者打满不影响生产者，路由查询慢不影响消息收发。
> 这和 **Servlet 中的线程池隔离**、**Dubbo 中的线程池隔离** 是同一个思想。

### 3. 统一的请求处理模板 — `addExecutor()`

```java
protected <V, T> void addExecutor(ExecutorService executor, ProxyContext context,
    V request, Runnable runnable, StreamObserver<T> responseObserver,
    Function<Status, T> statusResponseCreator) {

    // ① 执行 Pipeline（认证、鉴权、上下文初始化）
    if (request instanceof GeneratedMessageV3) {
        requestPipeline.execute(context, GrpcConstants.METADATA.get(Context.current()),
            (GeneratedMessageV3) request);
        validateContext(context);  // 校验 clientId 不为空
    }

    // ② 提交到对应线程池执行
    executor.submit(new GrpcTask<>(runnable, context, request, responseObserver,
        statusResponseCreator.apply(flowLimitStatus())));
}
```

> [!note] 模板方法模式
> `addExecutor()` 是一个**模板方法**，定义了请求处理的标准流程：
> 1. Pipeline 预处理
> 2. 上下文校验
> 3. 提交到线程池
>
> 每个 gRPC 接口方法（sendMessage、receiveMessage 等）都调用这个模板。

### 4. 请求方法实现模式

每个 gRPC 方法的实现都遵循**相同模式**：

```java
@Override
public void sendMessage(SendMessageRequest request,
    StreamObserver<SendMessageResponse> responseObserver) {

    // ① 定义异常响应构造器
    Function<Status, SendMessageResponse> statusResponseCreator =
        status -> SendMessageResponse.newBuilder().setStatus(status).build();

    // ② 创建上下文
    ProxyContext context = createContext();

    // ③ 提交到对应线程池（Producer 线程池）
    this.addExecutor(this.producerThreadPoolExecutor,
        context,
        request,
        () -> grpcMessagingActivity.sendMessage(context, request)
            .whenComplete((response, throwable) ->
                writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
        responseObserver,
        statusResponseCreator);
}
```

> [!note] CompletableFuture 异步处理
> `grpcMessagingActivity.sendMessage()` 返回 `CompletableFuture`，
> 通过 `whenComplete()` 异步写回响应，不阻塞 gRPC 线程。

### 5. 流量控制 — 拒绝策略

```java
protected class GrpcTaskRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (r instanceof GrpcTask) {
            GrpcTask grpcTask = (GrpcTask) r;
            // 直接返回 TOO_MANY_REQUESTS 响应，不丢弃请求
            writeResponse(grpcTask.context, grpcTask.request,
                grpcTask.executeRejectResponse,  // 预构建的拒绝响应
                grpcTask.streamObserver, null, null);
        }
    }
}
```

> [!important] 优雅的限流
> 当线程池满时，不是抛异常或丢弃请求，而是**返回一个预构建的 `TOO_MANY_REQUESTS` 响应**。
> 客户端收到后可以**重试或降级**。这是生产级限流的标准做法。

### 6. Telemetry — 双向流

```java
@Override
public StreamObserver<TelemetryCommand> telemetry(
    StreamObserver<TelemetryCommand> responseObserver) {

    // 创建双向流观察者
    ContextStreamObserver<TelemetryCommand> responseTelemetryCommand =
        grpcMessagingActivity.telemetry(responseObserver);

    return new StreamObserver<TelemetryCommand>() {
        @Override
        public void onNext(TelemetryCommand value) {
            // 每条消息都提交到 ClientManager 线程池
            addExecutor(clientManagerThreadPoolExecutor, ...);
        }
        @Override
        public void onError(Throwable t) { ... }
        @Override
        public void onCompleted() { ... }
    };
}
```

> [!tip] gRPC Streaming 的应用
> `telemetry` 是**双向流式 RPC**，用于：
> - 客户端定期上报心跳和配置变更
> - 服务端推送配置更新和流量调度指令
> - 长连接保持，减少频繁建连开销

---

## 🧩 核心设计总结

| 设计 | 在哪里 | 解决什么问题 |
|------|--------|-------------|
| **线程池隔离** | 5 个独立线程池 | 不同请求类型互不影响 |
| **Pipeline 模式** | `RequestPipeline` | 认证/鉴权/上下文初始化解耦 |
| **模板方法** | `addExecutor()` | 统一请求处理流程 |
| **异步处理** | `CompletableFuture` | 不阻塞 gRPC 线程 |
| **优雅限流** | `GrpcTaskRejectedExecutionHandler` | 线程池满返回标准错误码 |
| **双向流** | `telemetry()` | 长连接配置推送 |

---

## 💡 面试高频问题

**Q: 为什么用 5 个线程池而不是 1 个？**
A: 线程池隔离，防止某类请求（如长轮询的 receiveMessage）耗尽线程影响其他请求。

**Q: 线程池满了怎么办？**
A: 自定义拒绝策略，返回 `TOO_MANY_REQUESTS` 错误码，让客户端重试。

**Q: Pipeline 是怎么实现的？**
A: `RequestPipeline` 接口的 `pipe()` 方法返回一个新 lambda，先执行 source 再执行自己，形成链式调用。

---

> ⏭️ 下一篇：[[07-RequestPipeline]] — 责任链接口设计
