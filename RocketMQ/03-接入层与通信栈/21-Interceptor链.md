# Interceptor 链 - 三级拦截器体系

> 研究定位：只看 gRPC 进入业务前，Proxy 在 transport 层做了哪些预处理。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/interceptor/`、`GrpcServerBuilder.configInterceptor()`

## 先给结论

- Proxy 在 gRPC 层挂了 3 个拦截器。
- 实际执行顺序和注册顺序相反。
- 这些拦截器只管 transport 层，不管业务授权逻辑。
- 业务前置校验和认证授权，真正落在 `RequestPipeline`。

## 注册顺序

```java
serverBuilder
    .intercept(new GlobalExceptionInterceptor())
    .intercept(new ContextInterceptor())
    .intercept(new HeaderInterceptor());
```

## 实际执行顺序

1. `HeaderInterceptor`
2. `ContextInterceptor`
3. `GlobalExceptionInterceptor`

## 各自职责

### 1. `HeaderInterceptor`

它负责把连接属性变成可用的 gRPC Metadata：

- 远端地址
- 本地地址
- `CHANNEL_ID`
- Proxy Protocol 相关属性

它不是解析 TCP 字节流，而是读取 `ProxyAndTlsProtocolNegotiator` 已经塞进 `Attributes` 的信息。

### 2. `ContextInterceptor`

它把 Metadata 放进 gRPC `Context`：

```java
Context context = Context.current().withValue(GrpcConstants.METADATA, headers);
```

后面的 pipeline 就能通过 `GrpcConstants.METADATA.get(Context.current())` 取到头信息。

### 3. `GlobalExceptionInterceptor`

它包住整个调用链，负责把任何回调阶段抛出的异常转成 gRPC `Status`。

还做了两件关键事：

- 用 `ClosableServerCall` 防止重复 `close()`
- 对 `PERMISSION_DENIED` 这类错误不打完整堆栈

## 关键源码点

### `HeaderInterceptor`

- 先尝试从 `AttributeKeys.PROXY_PROTOCOL_ADDR/PORT` 取值
- 取不到再回退到 `Grpc.TRANSPORT_ATTR_REMOTE_ADDR`
- 用 `GrpcUtils.putHeaderIfNotExist()` 避免覆盖已有头

### `GlobalExceptionInterceptor`

- 会拦截 `onMessage`
- 会拦截 `onHalfClose`
- 也会拦截 `onCancel`、`onComplete`、`onReady`

这比只包一层 `try/catch` 更稳。

## 拦截器和 Pipeline 的边界

| 层 | 关注点 | 典型类 |
|---|---|---|
| Interceptor | 连接属性、Metadata、异常兜底 | `HeaderInterceptor` / `ContextInterceptor` / `GlobalExceptionInterceptor` |
| Pipeline | 业务前置处理、认证、鉴权、上下文初始化 | `ContextInitPipeline` / `AuthenticationPipeline` / `AuthorizationPipeline` |
| Activity | 真正的消息动作 | `SendMessageActivity` / `ReceiveMessageActivity` 等 |

## 扩展知识

- `ProxyAndTlsProtocolNegotiator` 负责把连接层信息放进 `Attributes`。
- `HeaderInterceptor` 只是把这些属性转成 Metadata。
- 这条链能把 transport 层和业务层解耦。

## 这篇最该记住的点

- Interceptor 处理“连接进入时”的共性逻辑。
- Pipeline 处理“请求进入后”的业务前置逻辑。
- 两者不是重复，而是分层。

## 关联阅读

- [[17-GrpcServerBuilder]]
- [[20-RequestPipeline]]
- [[19-GrpcMessagingApplication]]
