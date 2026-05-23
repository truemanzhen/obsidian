# Container 模式

> 研究定位：把 BrokerContainer 看成“多 Broker 共进程运行底座”来理解，回答它在 RocketMQ 5.5.0 里到底解决什么问题。
> 关键源码：`container/src/main/java/org/apache/rocketmq/container/BrokerContainer.java`、`BrokerContainerProcessor.java`、`BrokerContainerStartup.java`
> 阅读建议：这篇重点看进程内托管关系，不要把它误认为 Controller 或 Proxy。

## 先给结论

- Container 模式的核心是一个进程内托管多个 BrokerController。
- `BrokerContainer` 自己维护 remoting server、outer API、共享线程池和 broker controller 集合。
- 它让 master/slave/dLedger broker 可以在同一容器进程里被统一管理。
- 所以它解决的是进程组织问题，不是消息语义问题。

## `BrokerContainer` 的职责

它主要负责：

- 启动共享 remoting server
- 持有 `BrokerOuterAPI`
- 维护 master/slave/dLedger controller map
- 注册处理器
- 统一生命周期管理

这说明它更像：

- Broker 的宿主容器

## 为什么要有共享 remoting server

如果一个容器里放多个 broker 实例，很多基础设施没必要每个都单独建。

所以 Container 模式里会共享：

- remoting server
- fast remoting server
- 部分线程池

这样做的意义是：

- 降低进程级资源开销
- 统一入口

## 它和普通单 Broker 启动的区别

普通模式下：

- 一个进程基本对应一个 broker controller

Container 模式下：

- 一个进程可以管理多个 broker controller

所以关键差异不在消息链路，而在：

- 启动装配方式
- 生命周期管理方式

## 为什么不能把它和 Proxy 混掉

Container 是：

- Broker 托管模式

Proxy 是：

- 接入层协议适配与转发层

一个解决进程部署，一个解决接入协议。

## 哪些旧理解容易错

### “Container 模式就是 broker 集群模式”

不对。它是进程级托管模式。

### “Container 会改变消息处理主链”

不直接改变。它主要改变部署与生命周期组织。

## 这篇最值得记住的点

- `BrokerContainer` 是多 broker 共进程宿主。
- 它管理共享 remoting / outer API / controller 集合。
- 它不等于 Proxy，也不等于 Controller。

## 和后续笔记的关系

-  可以继续看容器模式下运维入口。
-  看角色切换时与 broker 组织的关系。

## 建议连读

1. [[59-HA主从同步设计]]
2. [[60-Controller与AutoSwitch主线]]