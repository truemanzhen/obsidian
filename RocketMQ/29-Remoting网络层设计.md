# Remoting 网络层设计

> 源码位置：`remoting/src/main/java/org/apache/rocketmq/remoting/`

## 🏗️ 整体架构

RocketMQ 的 Remoting 模块是整个系统的**通信基座**，所有组件（Client、Broker、NameServer、Proxy）之间的交互都基于它。

```
┌─────────────────────────────────────────────────────┐
│                  RemotingServer/Client               │
│         (NettyRemotingServer / NettyRemotingClient)  │
├─────────────────────────────────────────────────────┤
│              NettyRemotingAbstract                   │
│  ┌──────────┬──────────┬──────────┬──────────────┐  │
│  │processorTable│responseTable│semaphoreAsync│rpcHooks│
│  └──────────┴──────────┴──────────┴──────────────┘  │
├─────────────────────────────────────────────────────┤
│                   Netty Pipeline                     │
│  Handshake → TLS → Encoder/Decoder → IdleState →   │
│  ConnectionManage → ServerHandler                   │
├─────────────────────────────────────────────────────┤
│              RemotingCommand (协议)                   │
│  header(length|type|opaque|flag) + extFields + body │
└─────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 职责 |
|---|---|
| `NettyRemotingAbstract` | **核心基类**，封装请求分发、响应处理、信号量限流、RPC Hook |
| `NettyRemotingServer` | 服务端实现，管理 Boss/Selector 线程组、Channel Pipeline |
| `NettyRemotingClient` | 客户端实现，连接管理、重连机制 |
| `RemotingCommand` | **协议报文**，编解码、Header/Body 分离、自定义 Header 反射解析 |
| `NettyEncoder` / `NettyDecoder` | 编解码器，将 RemotingCommand 与 ByteBuf 互转 |
| `ResponseFuture` | 异步请求的 Future 封装，支持同步/异步/单向三种模式 |
| `NettyRequestProcessor` | 请求处理器接口，每个 RequestCode 对应一个 Processor |

## 📡 通信协议：RemotingCommand

### 报文格式

```
┌─────────────────────────────────────────────────┐
│                 RemotingCommand                  │
├──────────┬───────────────────────────────────────┤
│ 4 bytes  │ total length (header + body)          │
│ 4 bytes  │ header length | serialize type (高位) │
│ N bytes  │ header data (JSON 或 RocketMQ 自定义) │
│ M bytes  │ body data (业务 payload)              │
└──────────┴───────────────────────────────────────┘
```

**关键字段：**

```java
public class RemotingCommand {
    private int code;           // 请求码，如 SEND_MESSAGE=10, PULL_MESSAGE=11
    private LanguageCode language; // 客户端语言
    private int version;        // 版本号
    private int opaque;         // 请求ID，用于匹配 response
    private int flag;           // 0=请求, 1=响应; bit1=单向
    private String remark;      // 附加说明
    private HashMap<String, String> extFields; // 扩展字段（Header）
    private CommandCustomHeader customHeader;   // 自定义Header（反射解析）
    private byte[] body;        // 消息体
}
```

### 编解码流程

**编码（NettyEncoder）：**
1. `RemotingCommand.encode()` → 序列化 header 为 JSON 或 RocketMQ 自定义格式
2. 拼接 `4字节总长度 + 4字节header长度(含serializeType高位) + headerData + body`
3. 写入 ByteBuf

**解码（NettyDecoder）：**
1. 读取 4 字节 total length
2. 读取 4 字节 header length，低 24 位 = header 长度，高 8 位 = 序列化类型
3. 根据序列化类型反序列化 header
4. 剩余字节读为 body

### 序列化类型

```java
public enum SerializeType {
    JSON(0),                    // 默认，使用 fastjson2
    ROCKETMQ(1);               // RocketMQ 自定义二进制格式，更紧凑
}
```

## 🔄 三种通信模式

### 1. 同步调用（invokeSync）

```java
// 发送请求并阻塞等待响应
RemotingCommand response = remotingClient.invokeSync(addr, request, timeoutMillis);
```

**实现原理：**
1. 创建 `ResponseFuture`，注册到 `responseTable`（key = opaque）
2. `channel.writeAndFlush(request)` 发送
3. `ResponseFuture.waitResponse(timeout)` 阻塞
4. 收到响应时，`processResponseCommand` 通过 opaque 找到 future，`putResponse`

### 2. 异步调用（invokeAsync）

```java
// 发送请求，回调处理响应
remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
    @Override
    public void operationComplete(ResponseFuture responseFuture) {
        // 处理响应
    }
});
```

**实现原理：**
1. 创建 `ResponseFuture` 并设置 `InvokeCallback`
2. 注册超时任务（`timer.newTimeout`）
3. 响应到达时通过回调通知

### 3. 单向调用（invokeOneway）

```java
// 发送请求，不等待响应
remotingClient.invokeOneway(addr, request, timeoutMillis);
```

**实现原理：**
1. 设置 `flag` 标记为单向
2. `channel.writeAndFlush` 后直接返回
3. 服务端收到后不发送响应

### 信号量限流

```java
// 异步和单向请求都有信号量保护
protected final Semaphore semaphoreOneway;  // 默认 65535
protected final Semaphore semaphoreAsync;   // 默认 65535
```

- 同步调用无限流（因为会阻塞等待）
- 异步/单向通过信号量控制并发数，防止 OOM

## 🏭 服务端 Pipeline（NettyRemotingServer）

### Channel 初始化顺序

```java
protected ChannelPipeline configChannel(SocketChannel ch) {
    return ch.pipeline()
        .addLast(defaultEventExecutorGroup, "handshakeHandler", new HandshakeHandler())
        .addLast(defaultEventExecutorGroup,
            encoder,                    // 出站：RemotingCommand → ByteBuf
            new NettyDecoder(),         // 入站：ByteBuf → RemotingCommand
            distributionHandler,        // 流量统计
            new IdleStateHandler(0, 0, serverChannelMaxIdleTimeSeconds), // 空闲检测
            connectionManageHandler,    // 连接事件管理
            serverHandler               // 业务处理入口
        );
}
```

### 线程模型

```
┌───────────────────────────────────────────────────┐
│                 NettyRemotingServer               │
├───────────────────────────────────────────────────┤
│ eventLoopGroupBoss (1 thread)                     │
│   └── 接受新连接 (Accept)                          │
├───────────────────────────────────────────────────┤
│ eventLoopGroupSelector (N threads, 默认3)          │
│   └── I/O 读写 (Read/Write)                       │
├───────────────────────────────────────────────────┤
│ defaultEventExecutorGroup (M threads, 默认8)       │
│   └── 编解码 + 业务处理 (Codec + Handler)          │
├───────────────────────────────────────────────────┤
│ publicExecutor (K threads, 默认4)                  │
│   └── 回调执行 (Callback)                          │
└───────────────────────────────────────────────────┘
```

**Boss 线程**：只负责接受连接，1 个足够  
**Selector 线程**：负责网络 I/O，Linux 下自动使用 Epoll  
**Worker 线程**：编解码 + 请求分发，是主要的 CPU 消耗点  
**Public 线程**：异步回调执行

### Epoll 优化

```java
private boolean useEpoll() {
    return NetworkUtil.isLinuxPlatform()
        && nettyServerConfig.isUseEpollNativeSelector()
        && Epoll.isAvailable();
}
```

Linux 下自动切换 `EpollEventLoopGroup` + `EpollServerSocketChannel`，避免 JDK NIO 的 epoll bug。

## 📋 请求分发机制

### processorTable 注册

```java
// 启动时注册各 RequestCode 对应的 Processor
defaultRequestProcessorPair = new Pair<>(new AdminBrokerProcessor(), adminBrokerExecutor);
processorTable.put(RequestCode.SEND_MESSAGE, new Pair<>(sendMessageProcessor, sendExecutor));
processorTable.put(RequestCode.PULL_MESSAGE, new Pair<>(pullMessageProcessor, pullExecutor));
// ...
```

### 请求处理流程

```
1. processMessageReceived(ctx, msg)
   ├── REQUEST_COMMAND → processRequestCommand(ctx, msg)
   └── RESPONSE_COMMAND → processResponseCommand(ctx, msg)

2. processRequestCommand(ctx, cmd)
   ├── 从 processorTable 匹配 Processor（或用 defaultRequestProcessorPair）
   ├── 检查是否正在关闭（返回 GO_AWAY）
   ├── 检查是否拒绝请求（返回 SYSTEM_BUSY）
   ├── 提交到对应 ExecutorService 异步执行
   └── RejectedExecutionException → 返回 SYSTEM_BUSY

3. buildProcessRequestHandler(ctx, cmd, pair, opaque)
   ├── doBeforeRpcHooks(remoteAddr, cmd)  // 前置钩子
   ├── requestPipeline.execute(ctx, cmd)   // Pipeline 模式
   ├── pair.getObject1().processRequest(ctx, cmd) // 业务处理
   ├── doAfterRpcHooks(remoteAddr, cmd, response) // 后置钩子
   └── writeResponse(channel, cmd, response)       // 写回响应
```

**关键设计：**
- 每个 RequestCode 有独立的线程池，互不干扰
- 请求通过 `opaque` 匹配响应，支持多路复用
- RPC Hook 支持前置/后置拦截（用于 ACL 认证等）
- 优雅关闭时返回 `GO_AWAY` 响应码

## 📊 响应处理

### processResponseCommand

```java
public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
    final int opaque = cmd.getOpaque();
    final ResponseFuture responseFuture = responseTable.get(opaque);
    if (responseFuture != null) {
        responseFuture.setResponseCommand(cmd);
        responseFuture.release(); // 释放信号量
        responseTable.remove(opaque);

        if (responseFuture.getInvokeCallback() != null) {
            // 异步模式：执行回调
            executeInvokeCallback(responseFuture);
        } else {
            // 同步模式：唤醒等待线程
            responseFuture.putResponse(cmd);
        }
    }
}
```

### scanResponseTable（定时清理）

每秒扫描一次 `responseTable`，清理超时未响应的请求：
- 超时的 `ResponseFuture` 执行回调（返回超时异常）
- 防止内存泄漏

## 🔐 TLS 支持

```java
// 三种 TLS 模式
public enum TlsMode {
    DISABLED,   // 不使用 TLS
    PERMISSIVE, // 可选 TLS（客户端决定）
    ENFORCED;   // 强制 TLS
}
```

通过 `HandshakeHandler` + `TlsModeHandler` 实现动态 TLS 协商。

## 💡 设计亮点

1. **Pipeline 模式**：请求经过 RPC Hook → RequestPipeline → Processor，职责清晰
2. **三种通信模式**：同步/异步/单向，满足不同场景
3. **信号量限流**：防止异步请求 OOM
4. **opaque 多路复用**：单连接支持并发请求，通过 opaque 匹配响应
5. **优雅关闭**：GO_AWAY 响应码，让客户端平滑迁移
6. **Epoll 自适应**：Linux 下自动使用原生 Epoll

## 🔗 与其他模块的关系

- **Broker**：通过 `NettyRemotingServer` 对外提供服务，注册各 Processor
- **Client**：通过 `NettyRemotingClient` 连接 Broker/NameServer
- **Proxy**：`MultiProtocolRemotingServer` 同时支持 Remoting 和 gRPC 协议
- **NameServer**：独立的 `NettyRemotingServer` 实例

---

#设计模式 #核心类 #源码技巧
