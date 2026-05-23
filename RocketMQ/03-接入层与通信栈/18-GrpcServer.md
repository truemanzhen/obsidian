# GrpcServer - gRPC 服务器生命周期

> 研究定位：只看 gRPC Server 的启动、关闭和 TLS 热更新。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServer.java`、`proxy/src/main/java/org/apache/rocketmq/proxy/service/cert/TlsCertificateManager.java`

## 先给结论

- `GrpcServer` 只管生命周期，不管业务。
- 启动时先注册 TLS 监听器，再启动 gRPC Server。
- 关闭时先注销监听器，再优雅等待请求结束。
- TLS 热更新不是它自己轮询文件，而是靠 `TlsCertificateManager` 通知。

## 源码骨架

```java
public void start() throws Exception {
    tlsCertificateManager.registerReloadListener(this.tlsReloadHandler);
    this.server.start();
}

public void shutdown() {
    tlsCertificateManager.unregisterReloadListener(this.tlsReloadHandler);
    this.server.shutdown().awaitTermination(timeout, unit);
}
```

## 生命周期主线

### 启动

1. `TlsCertificateManager.registerReloadListener(...)`
2. `server.start()`

这意味着 TLS 热更新能力在服务启动后才进入工作态。

### 关闭

1. `unregisterReloadListener(...)`
2. `server.shutdown()`
3. `awaitTermination(timeout, unit)`

这样做是为了避免关闭后仍然收到证书重载回调。

## TLS 热更新

`GrpcTlsReloadHandler.onTlsContextReload()` 会调用：

`ProxyAndTlsProtocolNegotiator.loadSslContext()`

这一步重载的是 gRPC 侧的 SSL 上下文，不需要重启整个 Proxy。

## 证书监听不是单文件触发

`TlsCertificateManager` 的监听逻辑是双文件协调：

- 证书文件变更
- 私钥文件变更

只有两个都变更后才会通知 reload。

## 扩展知识

- 这套设计避免了“证书更新了一半”导致的中间态错误。
- `loadSslContext()` 可能走 OpenSSL，也可能走 JDK SSL Provider，取决于运行环境。
- 热更新失败通常只影响新连接，已有连接不一定立刻受影响。

## 这篇最该记住的点

- 生命周期和协议构建是分离的。
- TLS 热更新的触发源是文件监听，不是 Server 本身。
- `GrpcServer` 是优雅关闭的收口点。

## 关联阅读

- [[17-GrpcServerBuilder]]

- [[16-ConfigurationManager]]