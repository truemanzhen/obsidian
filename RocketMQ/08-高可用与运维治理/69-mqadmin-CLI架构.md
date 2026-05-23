# mqadmin CLI 架构

> 研究定位：把 `mqadmin` 的命令分发和子命令体系补成完整图景，回答“它为什么能同时管理 topic、broker、controller、ha 和 auth”。
> 关键源码：`tools/src/main/java/org/apache/rocketmq/tools/command/MQAdminStartup.java`、`SubCommand.java`
> 阅读建议：这篇是 [[68-Admin工具与运维命令]] 的架构补充。

## 先给结论

- `mqadmin` 不是一个单命令工具，而是统一命令框架。
- 所有子命令都通过 `SubCommand` 契约注册和执行。
- `MQAdminStartup` 负责把命令行参数解析成具体命令执行。

## 它的架构本质

可以把它理解成：

- 一个统一入口
- 一堆可插拔子命令
- 一套统一参数解析和 ACL 处理

## 为什么它重要

因为 RocketMQ 的很多运维工作都靠它完成：

- topic 管理
- consumer group 管理
- broker 配置
- controller 操作
- HA 状态查询
- ACL / auth 操作
- metrics / dump / query

所以 `mqadmin` 实际上是 RocketMQ 的控制面 CLI。

## 子命令的统一生命周期

每个子命令都要：

1. [[68-Admin工具与运维命令]]申明名字
2. [[65-ACL认证授权]]申明描述
3. 申明参数
4. 实现 execute

这让整个 CLI 架构保持一致。

## ACL 默认介入

如果没有显式传入 RPC hook，`MQAdminStartup` 会尝试从工具 ACL 配置加载 hook。

这说明：

- CLI 也在 ACL 管控范围内

## 哪些旧理解容易错

### “mqadmin 只是运维脚本”

不对。它是统一 CLI 架构。

### “命令之间彼此隔离，没有公共入口”

不对。它们共用同一个启动和调度框架。

## 这篇最值得记住的点

- `MQAdminStartup` 是统一入口。
- `SubCommand` 是命令契约。
- `mqadmin` 覆盖 RocketMQ 大部分控制面运维能力。

## 建议连读

1. [[68-Admin工具与运维命令]]
2. [[65-ACL认证授权]][[65-ACL认证授权]]