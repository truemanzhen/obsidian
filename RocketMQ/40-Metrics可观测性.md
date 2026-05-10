# Metrics 可观测性

> 源码位置：`broker/metrics/` + `proxy/metrics/` + `store/metrics/` + `remoting/metrics/`

## 🏗️ 整体架构

RocketMQ 5.x 基于 **OpenTelemetry** 构建了三层指标采集体系。

```
┌─────────────────────────────────────────────────────────┐
│                   Metrics 体系                           │
├──────────────┬──────────────┬────────────────────────────┤
│  Proxy 层    │  Broker 层   │  Store 层                  │
│  Metrics     │  Metrics     │  Metrics                   │
│  Manager     │  Manager     │  Manager                   │
├──────────────┴──────────────┴────────────────────────────┤
│              OpenTelemetry SDK                           │
│  ┌──────────┬──────────┬──────────┬──────────────────┐  │
│  │Counter   │Histogram │Gauge     │Exporter          │  │
│  │(计数器)  │(直方图)  │(仪表盘)  │(Prometheus/OTLP) │  │
│  └──────────┴──────────┴──────────┴──────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## 📦 核心类

| 类 | 层级 | 职责 |
|---|---|---|
| `BrokerMetricsManager` | Broker | Broker 级指标管理（消息收发、消费、延迟等） |
| `ProxyMetricsManager` | Proxy | Proxy 级指标管理（gRPC 请求、延迟等） |
| `DefaultStoreMetricsManager` | Store | Store 级指标管理（存储、刷盘、Compaction 等） |
| `RemotingMetricsManager` | Remoting | 网络层指标管理（RPC 延迟、连接数等） |
| `BrokerMetricsConstant` | - | 指标名称常量 |
| `StoreMetricsConstant` | - | Store 指标名称常量 |

## 📊 指标类型

### Counter（计数器）

```java
// 消息发送总量
LongCounter messagesInTotal;
// 指标名：rocketmq_messages_in_total
// Label：topic, is_system, message_type

// 消息消费总量
LongCounter messagesOutTotal;
// 指标名：rocketmq_messages_out_total
// Label：topic, consumer_group

// 提交消息总量
LongCounter commitMessagesTotal;
// 指标名：rocketmq_commit_messages_total
```

### Histogram（直方图）

```java
// 发送延迟
LongHistogram sendLatency;
// 指标名：rocketmq_send_latency
// Bucket：5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s

// 消费延迟
LongHistogram consumeLatency;
// 指标名：rocketmq_consume_latency

// RPC 延迟
LongHistogram rpcLatency;
// 指标名：rocketmq_rpc_latency
// Label：request_code, response_code, is_long_polling, result
```

### Gauge（仪表盘）

```java
// Consumer Group 数量
ObservableLongGauge consumerGroupCount;
// 指标名：rocketmq_consumer_group_count

// Topic 数量
ObservableLongGauge topicCount;
// 指标名：rocketmq_topic_count

// 消费延迟（最新 offset 差值）
ObservableLongGauge consumeLatency;
// 指标名：rocketmq_consume_offset_latency
// Label：topic, consumer_group, queue_id
```

## 🔌 导出方式

### 配置

```java
public enum MetricsExporterType {
    DISABLED,       // 不导出
    PROMETHEUS,     // Prometheus HTTP 端点
    OTLP_GRPC,      // OTLP gRPC 推送
    OTLP_LOGGING,   // OTLP JSON 日志输出
    LOGGER;          // SLF4J 日志输出
}
```

### Prometheus 导出

```java
// 启动 Prometheus HTTP Server
PrometheusHttpServer.builder()
    .setPort(5557)
    .build()
    .start();
```

### OTLP gRPC 导出

```java
// 推送到 OTLP Collector
OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
    .setEndpoint("http://collector:4317")
    .build();

SdkMeterProvider provider = SdkMeterProvider.builder()
    .registerMetricReader(PeriodicMetricReader.builder(exporter)
        .setInterval(Duration.ofSeconds(15))
        .build())
    .build();
```

## 📈 Broker 层关键指标

| 指标名 | 类型 | 说明 |
|---|---|---|
| `rocketmq_messages_in_total` | Counter | 消息发送总量 |
| `rocketmq_messages_out_total` | Counter | 消息消费总量 |
| `rocketmq_send_latency` | Histogram | 发送延迟分布 |
| `rocketmq_consume_latency` | Histogram | 消费延迟分布 |
| `rocketmq_pop_process_latency` | Histogram | Pop 消费处理延迟 |
| `rocketmq_long_polling_resume_latency` | Histogram | 长轮询恢复延迟 |
| `rocketmq_send_to_dlq_messages_total` | Counter | 进入死信队列消息数 |
| `rocketmq_consumer_group_count` | Gauge | 消费组数量 |

## 📈 Store 层关键指标

| 指标名 | 类型 | 说明 |
|---|---|---|
| `rocketmq_store_flush_latency` | Histogram | 刷盘延迟 |
| `rocketmq_store_put_message_latency` | Histogram | 消息写入延迟 |
| `rocketmq_store_get_message_latency` | Histogram | 消息读取延迟 |
| `rocketmq_compaction_latency` | Histogram | Compaction 延迟 |
| `rocketmq_timer_enqueue_latency` | Histogram | 延迟消息入队延迟 |

## 💡 设计亮点

1. **OpenTelemetry 标准**：统一指标采集框架
2. **多 Exporter 支持**：Prometheus/OTLP/Logger 灵活选择
3. **Label 维度丰富**：支持 topic、group、queue_id 等多维度查询
4. **零侵入**：通过 Hook 机制自动采集，业务无感知
5. **层级分明**：Proxy/Broker/Store 三层独立管理

## 🔗 与其他模块的关系

- **Remoting**：`RemotingMetricsManager` 采集 RPC 延迟
- **Broker**：`BrokerMetricsManager` 采集消息收发指标
- **Store**：`DefaultStoreMetricsManager` 采集存储指标
- **Proxy**：`ProxyMetricsManager` 采集 gRPC 指标

---

#核心类 #源码技巧
