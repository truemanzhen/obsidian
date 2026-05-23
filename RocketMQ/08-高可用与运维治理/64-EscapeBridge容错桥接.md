# EscapeBridge 容错桥接

> 研究定位：把 `EscapeBridge` 看成 slave acting master / remote escape 下的兜底写入桥，回答“本地主不可用时消息怎么绕出去”。
> 关键源码：`broker/src/main/java/org/apache/rocketmq/broker/failover/EscapeBridge.java`
> 阅读建议：这篇要和 [[59-HA主从同步设计]]、[[61-HA服务实现细节]] 一起看。

## 先给结论

- `EscapeBridge` 是 failover 场景下的写入桥。
- 它的核心用途是：当前 broker 不能本地落消息时，尝试把消息写到可用 broker。
- 这条链只在特定容错配置打开时才有意义。

## 它解决什么问题

当当前 broker 不能承担本地写入时，单纯返回失败会影响可用性。

所以 `EscapeBridge` 提供：

- 转发到远端 broker
- 保留消息继续可写的可能性

## 核心入口

### `putMessage(...)`

优先尝试：

- 当前 master broker

如果没有 master，并且允许：

- slave acting master
- remote escape

则转发给远端 broker。

### `asyncPutMessage(...)`

异步版本，逻辑和同步版一致，但返回 `CompletableFuture`。

## 为什么要有 `putMessageToSpecificQueue(...)`

因为有些场景需要把消息写到确定队列，而不是随机选队列。

所以它提供：

- 指定队列写入
- 异步指定队列写入

## 它和事务消息的关系

事务半消息在 failover 场景下也要特殊处理。

源码里会先判断：

- 是否是 half topic

如果是，就先构造事务消息再转发。

这说明容错桥接不是普通透传，它要理解部分消息语义。

## 为什么它重要

因为它把“写入失败”从单点失败变成：

- 可绕行
- 可远端逃逸

这在 slave acting master、remote escape 场景下很关键。

## 哪些旧理解容易错

### “EscapeBridge 只是工具类”

不对。它是容错写入主路径之一。

### “它只处理普通消息”

不对。事务半消息也会特殊处理。

## 这篇最值得记住的点

- `EscapeBridge` 是 failover 写入桥。
- 它依赖 slave acting master 和 remote escape 配置。
- 它不仅转发普通消息，也理解事务半消息。

## 建议连读

1. [[59-HA主从同步设计]]
2. [[61-HA服务实现细节]]