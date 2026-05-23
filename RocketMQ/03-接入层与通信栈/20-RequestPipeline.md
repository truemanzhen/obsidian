# RequestPipeline - 责任链接口设计

> 研究定位：只看 gRPC 请求在进入业务线程前，如何做统一前置处理。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/pipeline/RequestPipeline.java`、`ContextInitPipeline.java`、`AuthenticationPipeline.java`、`AuthorizationPipeline.java`

## 先给结论

- 这是一个函数式组合接口，不是带 `next` 指针的传统责任链。
- 它只处理 `GeneratedMessageV3` 这类 gRPC protobuf 请求。
- 它和 `ContextInterceptor` 不是同一层东西。
- 真正的执行顺序由 `pipe()` 的组合顺序反推出来。

## 接口骨架

```java
public interface RequestPipeline {
    void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request);

    default RequestPipeline pipe(RequestPipeline source) {
        return (ctx, headers, request) -> {
            source.execute(ctx, headers, request);
            execute(ctx, headers, request);
        };
    }
}
```

## 组合方式

```java
RequestPipeline pipeline = (context, headers, request) -> {};
pipeline = pipeline.pipe(new AuthorizationPipeline(...));
pipeline = pipeline.pipe(new AuthenticationPipeline(...));
pipeline = pipeline.pipe(new ContextInitPipeline());
```

最终执行顺序是：

1. `ContextInitPipeline`
2. `AuthenticationPipeline`
3. `AuthorizationPipeline`

## 三个默认实现

### `ContextInitPipeline`

把 gRPC Metadata 中的值写进 `ProxyContext`：

- `LOCAL_ADDRESS`
- `REMOTE_ADDRESS`
- `CLIENT_ID`
- `LANGUAGE`
- `CLIENT_VERSION`
- `SIMPLE_RPC_NAME`
- `NAMESPACE_ID`
- `remainingMs`

这一步还会读取 `Context.current().getDeadline()`。

### `AuthenticationPipeline`

如果 `authConfig.isAuthenticationEnabled()` 为真，就走认证评估器。

它还可能把用户名写回到 `AUTHORIZATION_AK` 头里，供后续链路使用。

### `AuthorizationPipeline`

如果 `authConfig.isAuthorizationEnabled()` 为真，就走授权评估器。

## 这个接口解决什么问题

- 统一前置处理入口
- 避免每个 RPC 方法自己写认证/鉴权
- 让 `GrpcMessagingApplication.addExecutor()` 只负责装配，不再内联重复前置逻辑

## 容易混淆的点

- `RequestPipeline` 是 gRPC 侧的管线。
- `remoting` 包下也有同名接口，但它是另一条链。
- `RequestPipeline` 不是网络拦截器，不负责解析 TCP/Metadata 注入。

## 扩展知识

- 这种写法本质上是函数组合，比传统责任链更轻。
- 如果 `authConfig` 为空，创建出来的 pipeline 会只剩上下文初始化。
- 这条链的输入是 protobuf 请求对象，所以对 HTTP 或 Remoting 请求不能直接复用。

## 关联阅读

- [[19-GrpcMessagingApplication]]

- [[16-ConfigurationManager]]