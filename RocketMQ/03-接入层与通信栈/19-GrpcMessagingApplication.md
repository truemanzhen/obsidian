# GrpcMessagingApplication - gRPC 服务实现核心

> 研究定位：只看 Proxy 的 gRPC 业务入口怎么分线程、怎么做前置处理、怎么返回错误。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/GrpcMessagingApplication.java`、`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/DefaultGrpcMessagingActivity.java`

## 先给结论

- 这是 Proxy 的业务入口，不是简单的 gRPC 壳。
- 它把不同 RPC 路由到不同线程池，隔离路由、生产、消费、客户端管理和事务。
- 所有请求都会先过 `RequestPipeline`，再进入具体业务。
- 线程池满时不会静默丢请求，而是返回 `TOO_MANY_REQUESTS`。

## 请求路由图

| RPC | 线程池 |
|---|---|
| `queryRoute` / `queryAssignment` | `routeThreadPoolExecutor` |
| `sendMessage` / `forwardMessageToDeadLetterQueue` / `recallMessage` | `producerThreadPoolExecutor` |
| `receiveMessage` / `ackMessage` / `changeInvisibleDuration` | `consumerThreadPoolExecutor` |
| `heartbeat` / `notifyClientTermination` / `syncLiteSubscription` / `telemetry` | `clientManagerThreadPoolExecutor` |
| `endTransaction` | `transactionThreadPoolExecutor` |

## 源码骨架

```java
public static GrpcMessagingApplication create(MessagingProcessor messagingProcessor) {
    RequestPipeline pipeline = (context, headers, request) -> {};
    pipeline = pipeline.pipe(new AuthorizationPipeline(authConfig, messagingProcessor))
        .pipe(new AuthenticationPipeline(authConfig, messagingProcessor))
        .pipe(new ContextInitPipeline());
    return new GrpcMessagingApplication(
        new DefaultGrpcMessagingActivity(messagingProcessor), pipeline);
}
```

## 核心主线

### 1. 创建阶段

`create()` 先组装 `RequestPipeline`，再创建 `DefaultGrpcMessagingActivity`。

注意这里的执行顺序是反的：

- 最后 `pipe` 的 `ContextInitPipeline` 最先执行
- 之后是 `AuthenticationPipeline`
- 最后是 `AuthorizationPipeline`

### 2. 请求进入

每个 RPC 方法都会：

1. 创建 `ProxyContext`
2. 调用 `addExecutor(...)`
3. 先执行 pipeline
4. 再提交线程池

### 3. 上下文校验

`validateContext(context)` 只检查一件事：

- `clientID` 不能为空

这也是 `GrpcMessagingApplicationTest` 里验证的重点。

## 失败处理

### 线程池拒绝

`GrpcTaskRejectedExecutionHandler` 会直接写回预构造的错误响应，不会丢请求。

### 业务异常

`writeResponse(...)` 会把异常转成 gRPC `Status`，再写回给客户端。

### 客户端取消

`ResponseWriter` 会检查 `ServerCallStreamObserver.isCancelled()`，避免继续回写。

## 这个类真正新增了什么

5.5.0 的这个入口不只是 `sendMessage` / `receiveMessage`，还包括：

- `queryAssignment`
- `notifyClientTermination`
- `syncLiteSubscription`
- `telemetry`
- `recallMessage`
- `forwardMessageToDeadLetterQueue`

这说明 gRPC 侧已经不只是“消息收发”，而是完整接入面。

## 扩展知识

- `receiveMessage` 是流式返回，不是标准的 `whenComplete` 模式。
- `telemetry` 是双向流，仍然会走客户端管理线程池。
- `DefaultGrpcMessagingActivity` 负责把具体能力拆到 route / producer / consumer / client / transaction 等活动类里。

## 关联阅读

- [[20-RequestPipeline]]
- [[21-Interceptor链]]