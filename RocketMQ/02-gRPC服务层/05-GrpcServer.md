# GrpcServer — gRPC 服务器生命周期

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServer.java`
> 🏷️ #核心类

## 🎯 类的职责

封装 gRPC `Server` 对象，实现 `StartAndShutdown` 接口，管理服务器的启动和关闭。
同时负责 **TLS 证书热更新**的注册。

## 🔍 源码逐行

```java
public class GrpcServer implements StartAndShutdown {
    private final Server server;                    // gRPC Server 实例
    private final long timeout;                     // 优雅关闭超时时间
    private final TimeUnit unit;                    // 超时时间单位
    private final TlsCertificateManager tlsCertificateManager;  // TLS 证书管理器
    final GrpcTlsReloadHandler tlsReloadHandler;    // TLS 热更新处理器

    // 启动
    public void start() throws Exception {
        // ① 注册 TLS 证书变更监听器
        tlsCertificateManager.registerReloadListener(this.tlsReloadHandler);
        // ② 启动 gRPC 服务器
        this.server.start();
    }

    // 关闭
    public void shutdown() {
        // ① 注销 TLS 监听器
        tlsCertificateManager.unregisterReloadListener(this.tlsReloadHandler);
        // ② 优雅关闭（等待进行中的请求完成）
        this.server.shutdown().awaitTermination(timeout, unit);
    }

    // TLS 证书热更新处理器（内部类）
    class GrpcTlsReloadHandler implements TlsCertificateManager.TlsContextReloadListener {
        @Override
        public void onTlsContextReload() {
            // 重新加载 SSL 上下文（不停机）
            ProxyAndTlsProtocolNegotiator.loadSslContext();
        }
    }
}
```

## 💡 设计亮点

### TLS 证书热更新

```
┌─────────────────┐     证书变更通知      ┌──────────────┐
│ TlsCertificate  │ ──────────────────→  │ GrpcServer   │
│ Manager         │                      │              │
│ (监控证书文件)   │                      │ onTlsContext │
│                 │                      │ Reload()     │
└─────────────────┘                      └──────┬───────┘
                                                │
                                        ┌───────▼────────┐
                                        │ loadSslContext()│
                                        │ (不停机更新TLS) │
                                        └────────────────┘
```

> [!tip] 为什么需要热更新？
> 生产环境中，TLS 证书有有效期（通常 1 年）。如果更新证书需要重启服务器，会造成连接中断。
> 通过监听证书文件变更，**运行时替换 SSL 上下文**，新连接使用新证书，老连接不受影响。

### 观察者模式

```java
// 注册监听器
tlsCertificateManager.registerReloadListener(this.tlsReloadHandler);

// 证书变更时通知所有监听器
// TlsCertificateManager 内部遍历 listeners → listener.onTlsContextReload()
```

这是经典的**观察者模式**（Listener 模式），在中间件中广泛使用。

---

> ⏭️ 下一篇：[[06-GrpcMessagingApplication]] — gRPC 服务实现 + 线程池隔离
