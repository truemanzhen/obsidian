# Metrics 可观测性

> 研究定位：把 RocketMQ 5.5.0 的 BrokerMetricsManager、Store metrics 和 OpenTelemetry 链路拆开，回答“指标到底从哪里来、怎么出去”。
> 关键源码：`broker/src/main/java/org/apache/rocketmq/broker/metrics/BrokerMetricsManager.java`、`store/src/main/java/org/apache/rocketmq/store/metrics/DefaultStoreMetricsManager.java`
> 阅读建议：这篇重点看指标定义和标签语义，不要把它和 Trace 混成一件事。

## 先给结论

- RocketMQ 的 metrics 是 Broker、Store、Remoting 多层共同组成的观测体系。
- `BrokerMetricsManager` 负责 broker 侧指标注册、标签和导出。
- `DefaultStoreMetricsManager` 负责存储侧指标。
- 5.5.0 已经支持 OpenTelemetry、Prometheus 和 logging exporter 等不同导出方式。

## `BrokerMetricsManager` 的角色

它是 broker 侧总入口，负责：

- 创建 meter
- 定义 counter / histogram / gauge
- 绑定 label
- 启动导出器

所以它不是“打印一些统计值”，而是完整指标编排器。

## 指标分哪几类

### 1. [[25-Broker核心处理器]]Broker 状态类

- processor watermark
- broker permission
- topic 数量
- consumer group 数量

### 2. [[68-Admin工具与运维命令]] [[68-Admin工具与运维命令]]请求吞吐类

- messages in/out
- throughput in/out
- message size

### 3. 连接类

- producer connections
- consumer connections

### 4. 消费滞后类

- lag messages
- lag latency
- inflight messages
- queueing latency
- ready messages

### 5. 事务类

- half messages
- commit / rollback total
- transaction finish latency

## 标签为什么重要

`BrokerMetricsManager` 会把 metrics 按上下文打标签，例如：

- cluster
- node type
- topic
- consumer group
- consume mode
- version

这意味着 metrics 不是纯数值，而是带语义维度的观测数据。

## Store metrics 和 Broker metrics 的关系

Store 负责底层存储统计，例如：

- 文件、刷盘、吞吐、存储侧状态

Broker 负责更靠近协议和消费语义的统计。

所以二者是上下层关系，不是重复实现。

## 为什么它和 Trace 不能混

- Trace 关注“某条消息经历了什么”
- Metrics 关注“整体系统发生了什么”

一个是样本级，一个是聚合级。

## OpenTelemetry 在这里扮演什么

它是导出和兼容层。

RocketMQ 不是只做本地内存统计，而是能把指标推给：

- Prometheus
- OTLP
- logging exporter

## 哪些旧理解容易错

### “Metrics 就是日志”

不对。它是结构化指标。

### “Metrics 和 Trace 是同一套东西”

不对。目标和粒度不同。

### “Broker metrics 足够代表全部系统”

不够。Store 和 remoting 也有自己的指标链。

## 这篇最值得记住的点

- `BrokerMetricsManager` 是 broker 侧指标总控。
- Store 也有独立 metrics 管理。
- metrics 和 trace 是两个不同的观测层。

## 和后续笔记的关系

-  讲消息级链路。
-  讲怎么在运维上消费这些信息。

## 建议连读

1. [[25-Broker核心处理器]]
2. [[68-Admin工具与运维命令]]