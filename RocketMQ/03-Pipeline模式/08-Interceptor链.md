# Interceptor 链 — 三级拦截器体系

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/interceptor/`
> 🏷️ #设计模式 #核心类

## 🎯 整体设计

Proxy 在 gRPC 层设置了**三级拦截器**，在请求到达业务逻辑之前进行预处理。

## 📐 执行顺序

```
gRPC 请求进入
    │
    ▼
┌─────────────────────────────────────┐
│ ① HeaderInterceptor                 │
│    解析远程/本地地址、Channel ID      │
│    支持 HAProxy Protocol             │
└──────────────────┬──────────────────┘
                   ▼
┌─────────────────────────────────────┐
│ ② ContextInterceptor                │
│    将 Metadata 注入 gRPC Context     │
│    供后续 Pipeline 读取              │
└──────────────────┬──────────────────┘
                   ▼
┌─────────────────────────────────────┐
│ ③ GlobalExceptionInterceptor        │
│    包裹整个调用链                     │
│    捕获所有异常并转换为 gRPC Status   │
└──────────────────┬──────────────────┘
                   ▼
            业务逻辑处理
```

> [!important] 为什么是反序？
> gRPC 拦截器的注册顺序和执行顺序是**相反**的：
> ```java
> serverBuilder
>     .intercept(new GlobalExceptionInterceptor())  // 注册第1 → 执行第3（最外层）
>     .intercept(new ContextInterceptor())           // 注册第2 → 执行第2
>     .intercept(new HeaderInterceptor());           // 注册第3 → 执行第1（最内层）
> ```
> 类似**洋葱模型**，`GlobalExceptionInterceptor` 是最外层的"壳"。

---

## 🔍 逐个解析

### 1. HeaderInterceptor — 请求头解析

```java
public class HeaderInterceptor implements ServerInterceptor {
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call, Metadata headers, ServerCallHandler<R, W> next) {

        // ① 获取远程地址（支持 Proxy Protocol 传递真实 IP）
        String remoteAddress = getProxyProtocolAddress(call.getAttributes());
        if (StringUtils.isBlank(remoteAddress)) {
            // 从 Netty 连接属性中获取
            SocketAddress remoteSocketAddress = call.getAttributes()
                .get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            remoteAddress = parseSocketAddress(remoteSocketAddress);
        }
        GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.REMOTE_ADDRESS, remoteAddress);

        // ② 获取本地地址
        SocketAddress localSocketAddress = call.getAttributes()
            .get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);
        GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.LOCAL_ADDRESS, localAddress);

        // ③ 透传 HAProxy Protocol 属性
        for (Attributes.Key<?> key : call.getAttributes().keys()) {
            if (StringUtils.startsWith(key.toString(), HAProxyConstants.PROXY_PROTOCOL_PREFIX)) {
                // 将 Proxy Protocol 属性注入到 Metadata
            }
        }

        // ④ 传递 Channel ID
        String channelId = call.getAttributes().get(AttributeKeys.CHANNEL_ID);
        GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.CHANNEL_ID, channelId);

        return next.startCall(call, headers);
    }
}
```

> [!tip] Proxy Protocol 支持
> 当 Proxy 前面有 HAProxy 等负载均衡器时，客户端的真实 IP 会被 LB 的 IP 覆盖。
> **Proxy Protocol** 允许 LB 在 TCP 连接建立时传递真实客户端 IP。
> 这个拦截器负责从 Netty 的 Attributes 中提取这些信息。

### 2. ContextInterceptor — Context 注入

```java
public class ContextInterceptor implements ServerInterceptor {
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call, Metadata headers, ServerCallHandler<R, W> next) {

        // 将 headers 注入到 gRPC Context 中
        Context context = Context.current().withValue(GrpcConstants.METADATA, headers);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
```

> [!note] gRPC Context 机制
> gRPC 的 `Context` 类似 `ThreadLocal`，但它是**请求级别**的，且在异步调用间自动传播。
> 将 `Metadata`（包含 HeaderInterceptor 注入的地址信息）存入 Context，
> 后续的 Pipeline 和业务代码就可以通过 `GrpcConstants.METADATA.get(Context.current())` 获取。

### 3. GlobalExceptionInterceptor — 全局异常兜底

```java
public class GlobalExceptionInterceptor implements ServerInterceptor {
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call, Metadata headers, ServerCallHandler<R, W> next) {

        // 包装 ServerCall，防止重复 close
        final ServerCall<R, W> serverCall = new ClosableServerCall<>(call);
        ServerCall.Listener<R> delegate = next.startCall(serverCall, headers);

        // 包装 Listener 的每个回调方法，统一 try-catch
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<R>(delegate) {
            @Override
            public void onMessage(R message) {
                try { super.onMessage(message); }
                catch (Throwable e) { closeWithException(e); }
            }

            @Override
            public void onHalfClose() {
                try { super.onHalfClose(); }
                catch (Throwable e) { closeWithException(e); }
            }

            // onCancel, onComplete, onReady 同理...

            private void closeWithException(Throwable t) {
                Status status = Status.INTERNAL.withDescription(t.getMessage());
                boolean printLog = true;

                if (t instanceof StatusRuntimeException) {
                    status = ((StatusRuntimeException) t).getStatus();
                    // 权限拒绝不打印错误日志（安全考虑）
                    if (status.getCode().value() == Status.PERMISSION_DENIED.getCode().value()) {
                        printLog = false;
                    }
                }
                if (printLog) {
                    log.error("grpc server has exception. errorMsg:{}, e:", t.getMessage(), t);
                }
                serverCall.close(status, trailers);
            }
        };
    }
}
```

> [!important] 防重复关闭
> ```java
> private static class ClosableServerCall<R, W> extends ForwardingServerCall... {
>     private boolean closeCalled = false;
>
>     @Override
>     public synchronized void close(final Status status, final Metadata trailers) {
>         if (!closeCalled) {
>             closeCalled = true;
>             super.close(status, trailers);
>         }
>     }
> }
> ```
> 多个拦截器或业务代码可能同时调用 `close()`，`ClosableServerCall` 确保只关闭一次，避免 gRPC 报错。

---

## 🧩 拦截器 vs Pipeline — 职责划分

```
┌─────────────────────────────────────────────────────┐
│                    请求处理流程                       │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │ gRPC 拦截器层（Header → Context → Exception）│   │
│  │ 职责：网络层信息提取、异常兜底                 │   │
│  └──────────────────┬───────────────────────────┘   │
│                     ▼                               │
│  ┌──────────────────────────────────────────────┐   │
│  │ Pipeline 层（ContextInit → AuthN → AuthZ）    │   │
│  │ 职责：业务层认证鉴权、上下文构建               │   │
│  └──────────────────┬───────────────────────────┘   │
│                     ▼                               │
│  ┌──────────────────────────────────────────────┐   │
│  │ 业务逻辑层（GrpcMessagingActivity）           │   │
│  │ 职责：消息处理                                 │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

| 层 | 关注点 | 技术 |
|----|--------|------|
| 拦截器 | 网络层：地址、连接、异常 | gRPC ServerInterceptor |
| Pipeline | 业务层：认证、鉴权 | 自定义 RequestPipeline |
| 业务层 | 领域逻辑：消息收发 | GrpcMessagingActivity |

> [!tip] 为什么要分两层？
> - 拦截器是 **gRPC 原生机制**，适合处理网络层的通用逻辑（地址解析、异常兜底）
> - Pipeline 是 **自定义机制**，适合处理业务层的通用逻辑（认证、鉴权）
> - 两层各司其职，互不耦合

---

## 💡 学习收获

1. **洋葱模型**：拦截器层层包裹，内层异常被外层捕获
2. **防重复关闭**：`ClosableServerCall` 用 `synchronized + boolean` 保证幂等
3. **分层设计**：gRPC 拦截器处理网络层，Pipeline 处理业务层
4. **安全日志**：权限拒绝不打印堆栈（防止信息泄露）

---

> ⏭️ 下一篇：消息处理层 — SendMessage / ReceiveMessage / AckMessage（待续）
