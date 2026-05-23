# Remoting 网络层设计

> 研究定位：把 RocketMQ 的 Remoting 当成“协议与请求分发底座”来读，而不是只当成一层 Netty 包装。
> 关键源码：`remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java`、`NettyRemotingServer.java`、`NettyEncoder.java`、`NettyDecoder.java`、`protocol/RemotingCommand.java`

## 先给结论

- Remoting 是 RocketMQ 多个组件共享的通信底座，不只属于 Broker。
- `NettyRemotingAbstract` 才是主骨架，`NettyRemotingServer` 和 `NettyRemotingClient` 只是两侧实现。
- `RemotingCommand` 把请求码、扩展头、序列化类型和 body 统一成一个协议对象。
- 请求分发的核心不是“收到包就调 processor”，而是“先匹配 request code，再过 hook/pipeline，再提交线程池”。

## 先拆四个核心对象

### 1. `RemotingCommand`

它回答：

- 远端到底发来了什么
- 这个包是请求还是响应
- header 和 body 怎么拆

### 2. `NettyRemotingAbstract`

它回答：

- 收到请求后怎么分发
- 收到响应后怎么找到对应 future
- 异步/单向并发怎么限流

### 3. `NettyRemotingServer`

它回答：

- 监听端口怎么起来
- Netty pipeline 怎么组
- 服务端线程模型怎么配

### 4. `NettyEncoder` / `NettyDecoder`

它们回答：

- `RemotingCommand` 如何转成字节
- 字节如何还原成 `RemotingCommand`

## `RemotingCommand` 的协议地位

关键字段包括：

- `code`
- `language`
- `version`
- `opaque`
- `flag`
- `remark`
- `extFields`
- `body`

这里最关键的是两个点：

- `opaque` 用来做请求-响应关联
- `extFields` 是 RocketMQ 很多扩展语义的承载面

### 编解码主线

`NettyEncoder.encode()` 做的事情很克制：

```java
remotingCommand.fastEncodeHeader(out);
if (body != null) {
    out.writeBytes(body);
}
```

`NettyDecoder` 则先按长度切帧，再调：

```java
RemotingCommand cmd = RemotingCommand.decode(frame);
cmd.setProcessTimer(timer);
```

这说明：

- 帧边界是 Netty decoder 负责
- 协议解释是 `RemotingCommand` 自己负责

## `NettyRemotingAbstract` 才是主干

它内部最重要的几个成员是：

- `responseTable`
- `processorTable`
- `semaphoreOneway`
- `semaphoreAsync`
- `rpcHooks`
- `requestPipeline`

### 收包入口

```java
public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
    switch (msg.getType()) {
        case REQUEST_COMMAND:
            processRequestCommand(ctx, msg);
            break;
        case RESPONSE_COMMAND:
            processResponseCommand(ctx, msg);
            break;
    }
}
```

这里已经把协议层拆成两条线：

- 请求线
- 响应线

## 请求分发主线

当收到请求时，核心流程是：

1. 按 `request code` 从 `processorTable` 找处理器
2. 找不到则走默认处理器
3. 执行 `doBeforeRpcHooks(...)`
4. 执行 `requestPipeline`
5. 把业务处理提交给对应线程池
6. 处理完后 `writeResponse(...)`
7. 执行 `doAfterRpcHooks(...)`

这意味着 RocketMQ 的 Remoting 不是“裸 processor 表”，而是：

- hook
- pipeline
- processor
- response writeback

共同组成的框架。

## 响应处理主线

响应侧的核心是 `responseTable`。

它以 `opaque` 为 key 保存未完成请求，等响应回来后完成：

- 同步等待
- 异步回调
- 信号量释放

所以 `opaque` 不是普通流水号，而是整套多路复用的锚点。

## 并发控制不是靠线程池一层

`NettyRemotingAbstract` 还有两道信号量：

- `semaphoreOneway`
- `semaphoreAsync`

它们保护的是：

- 单向请求并发数
- 异步请求并发数

这和业务线程池是两层不同限流：

- 信号量控制“飞行中的请求数”
- 线程池控制“处理中的任务数”

## `NettyRemotingServer` 关心的是服务端载体

它负责：

- 创建 boss / selector event loop
- 创建 `DefaultEventExecutorGroup`
- 初始化 `ServerBootstrap`
- 绑定监听端口
- 安排扫描超时响应表和 housekeeping

### 线程模型

可以粗分成：

- boss：accept 连接
- selector：I/O 读写
- defaultEventExecutorGroup：编解码与 handler
- publicExecutor：回调执行

这里和 Proxy gRPC 那套线程池隔离是两种不同层次：

- Remoting 更偏协议和 handler 层
- Proxy gRPC 更偏业务能力分类

## 它为什么对 Proxy 也重要

虽然 Proxy 在 5.x 主要暴露 gRPC，但仍然有：

- `RemotingProtocolServer`
- 4.x 协议兼容

所以如果只学 Proxy/gRPC 而忽略 Remoting，会漏掉：

- legacy client 兼容
- Broker/NameServer/Admin 命令风格
- 统一的 request/response 处理骨架

## 容易误解的点

- `RemotingCommand` 不只是 Broker 内部协议，很多组件都依赖它。
- `NettyRemotingServer` 不是全部逻辑中心，真正公共逻辑在 `NettyRemotingAbstract`。
- Remoting 的 `requestPipeline` 和 Proxy gRPC 的 `RequestPipeline` 不是一回事，只是命名相似。

## 关联阅读

- [[14-ProxyStartup]]
- [[19-GrpcMessagingApplication]]