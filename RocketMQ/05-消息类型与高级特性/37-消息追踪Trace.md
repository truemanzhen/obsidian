# 消息追踪 Trace

> 研究定位：把 RocketMQ client trace 的真实实现链拆开，回答“Trace 是 Broker 功能还是客户端自发再发一条消息”。
> 关键源码：`client/src/main/java/org/apache/rocketmq/client/trace/TraceContext.java`、`TraceBean.java`、`AsyncTraceDispatcher.java`、`TraceDataEncoder.java`
> 阅读建议：这篇重点看客户端 trace 发送机制，不要先假设它是 Broker 内建存储。

## 先给结论

- RocketMQ 的 trace 不是 Broker 自动帮你记全链路，而是客户端 hook 采集后再异步发送一批 trace 消息。
- Trace 的最核心对象是 `TraceContext` 和 `TraceBean`。
- 实际分发执行者是 `AsyncTraceDispatcher`，它内部有队列、线程池和独立的 trace producer。
- 默认 trace topic 是系统 topic `RMQ_SYS_TRACE_TOPIC`。

## `TraceContext` 和 `TraceBean` 分工

### `TraceContext`

它表示一次 trace 事件上下文，通常承载：

- traceType
- timestamp
- groupName
- costTime
- success / code
- message 列表

### `TraceBean`

它表示单条消息维度的 trace 信息，例如：

- topic
- msgId
- tags
- keys
- storeHost
- bodyLength

所以可以理解成：

- `TraceContext` 是一次事件
- `TraceBean` 是事件下的消息项

## `AsyncTraceDispatcher` 到底做了什么

它不是简单线程包装，而是完整异步发送器。

核心组件包括：

- `traceContextQueue`
- `appenderQueue`
- `traceExecutor`
- `traceProducer`
- `worker`

这说明 trace 发送不是同步阻塞主业务线程的。

## Trace 是怎么被发出去的

典型链路是：

1. [[28-CommitLog写入流程]]hook 采集 trace context
2. [[67-Metrics可观测性]] [[29-ConsumeQueue设计]]`append(...)` 放进 `traceContextQueue`
3. worker 线程周期性 `flushTraceContext(...)`
4. 编码成 trace 消息
5. 用独立 `traceProducer` 发到 trace topic

所以 Trace 本质上仍然是：

- 普通 RocketMQ 消息

只是 topic 和消息体格式不同。

## 独立 trace producer 的意义

`AsyncTraceDispatcher` 会创建自己的 `DefaultMQProducer`：

- 设置独立 group
- 关闭 trace 的 trace
- 限制最大消息体

这说明 trace 体系在实现上是：

- “业务客户端旁路再发一条系统消息”

而不是复用主发送调用栈直到最底层。

## 为什么它必须异步

如果 trace 发送与主业务消息同步绑定：

- 主链路延迟会变差
- trace 失败会污染业务成功率

所以 RocketMQ 的 trace 设计很明确：

- 可以丢
- 要异步
- 尽量不影响主业务

这也是 `traceContextQueue` 满了会直接累计丢弃计数的原因。

## Trace 数据最终长什么样

`TraceDataEncoder` 会把 `TraceContext` 和 `TraceBean` 编成字符串/消息体，再发到 trace topic。

所以你在 Broker 侧看到的 trace 数据，不是结构化表，而是：

- 一种约定格式的系统消息

## 它和 OpenTelemetry / Metrics 的关系

不要混淆：

- Trace：消息级链路记录
- Metrics：聚合指标
- OpenTelemetry：更广义观测协议整合

Trace 更偏：

- 哪条消息发了、收了、耗时多少

不是 Broker 总览指标。

## 哪些旧理解容易错

### “Trace 是 Broker 自动记录的”

不对。主实现位于 client 侧。

### “打开 trace 后业务线程直接同步写 trace”

不对。核心路径是异步队列 + 独立 producer。

### “Trace 和业务消息走完全独立存储系统”

不对。它本质上还是发到 RocketMQ 的系统 topic。

## 这篇最值得记住的点

- Trace 是客户端旁路消息。
- `AsyncTraceDispatcher` 是核心执行器。
- `TraceContext` 和 `TraceBean` 是最重要的数据模型。

## 和后续笔记的关系

-  负责系统级指标与观测。
- [[67-Metrics可观测性]] 可继续看运维侧如何排查链路。

## 建议连读

1. [[28-CommitLog写入流程]]
2. [[67-Metrics可观测性]]