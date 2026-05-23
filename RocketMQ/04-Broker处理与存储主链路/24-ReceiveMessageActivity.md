# ReceiveMessageActivity

> 研究定位：回答 5.x gRPC `receiveMessage` 到底是不是“传统拉取换个接口”，以及 POP、Lite、长轮询、自动续期在这里怎么汇合。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ReceiveMessageActivity.java`

## 先给结论

- `receiveMessage` 走的是 POP/gRPC 消费语义，不是经典 pull 直接透传。
- 它会同时处理：长轮询、过滤表达式、Lite consumer 分支、FIFO 标记、自动续期。
- `autoRenew` 打开时，请求里的 invisible duration 可能被 Proxy 默认值替换。
- 返回不是单个普通响应对象，而是通过 `ReceiveMessageResponseStreamWriter` 以流式方式写回。

## 源码骨架

```java
if (isLite) {
    popFuture = this.messagingProcessor.popLiteMessage(...);
} else {
    popFuture = this.messagingProcessor.popMessage(..., fifo, ...);
}

popFuture.thenAccept(popResult -> {
    Runnable doAfterWrite = null;
    if (autoRenew) {
        doAfterWrite = handleAutoRenew(...);
    }
    writer.writeAndComplete(ctx, request, popResult, doAfterWrite);
});
```

## 主链路

### 1. 读取客户端设置

它先从 `GrpcClientSettingsManager` 取：

- `clientType`
- `subscription`
- `fifo`
- `backoffPolicy.maxAttempts`
- `requestTimeout`

也就是说，消费请求不是只看当前 request，还要结合之前注册的 client settings。

### 2. 计算长轮询时间

长轮询时间来源有两种：

- 请求显式带了 `longPollingTimeout`
- 没带时，用 `remainingMs - requestTimeout/2`

然后再夹在配置区间里：

- `grpcClientConsumerMinLongPollingTimeoutMillis`
- `grpcClientConsumerMaxLongPollingTimeoutMillis`

### 3. 做 topic/group/visible/filter 校验

这里会处理：

- topic + consumerGroup 校验
- invisible time 校验
- filter expression -> `SubscriptionData`

如果 filter 表达式非法，直接返回 `ILLEGAL_FILTER_EXPRESSION`。

### 4. Lite 与普通 POP 分流

Lite 分支会多做两件事：

- 确认 `GrpcClientChannel` 仍然存在
- 校验当前 channel 未确认消息数不能超过阈值

之后才调用 `popLiteMessage(...)`。

普通分支则走 `popMessage(...)`，并显式带上：

- `ConsumeInitMode.MAX`
- `fifo`

### 5. 自动续期

如果：

- `enableProxyAutoRenew`
- 且请求打开了 `autoRenew`

那么 Proxy 会在消息成功返回后，把每条消息的 `PROPERTY_POP_CK` 包成 `MessageReceiptHandle` 注册起来。

## `autoRenew` 会改写什么

这是一个容易忽略的点。

当自动续期开启时，实际 invisible time 不再直接使用请求值，而是：

- `proxyConfig.getDefaultInvisibleTimeMills()`

所以 `autoRenew` 不只是“多做一步续期”，它还会改变初始不可见时间策略。

## 长轮询边界的一个细节

如果计算出来的 `pollingTime` 大于当前 deadline 剩余时间：

- 若剩余时间还大于最小长轮询时间，就退化为剩余时间
- 否则直接返回非法轮询时间错误

并且这里对旧客户端做了兼容：

- 旧版本可能返回 `BAD_REQUEST`
- 新版本返回 `ILLEGAL_POLLING_TIME`

## 队列选择并不复杂，但语义很重要

`ReceiveMessageQueueSelector` 的策略是：

1. 如果请求里带了 broker name，优先命中该 broker
2. 否则从读队列里选一个可读 queue

这说明 gRPC receive 仍然会感知 broker 定向消费，而不只是“盲拉一条消息”。

## 容易误解的点

- `receiveMessage` 不是经典 `PullMessageProcessor` 的直接映射。
- Lite consumer 和普通 consumer 不是同一条无差别路径。
- 自动续期不是 Broker 自己全权处理，Proxy 也参与了 receipt handle 生命周期管理。
- FIFO 标记是在 POP 路径里显式传给后端的，不是靠 topic 名字猜。

## 关联阅读