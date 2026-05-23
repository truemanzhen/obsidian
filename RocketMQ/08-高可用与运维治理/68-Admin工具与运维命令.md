# Admin 工具与运维命令

> 研究定位：把 RocketMQ 的运维命令入口、命令注册和执行链路讲清楚，回答“mqadmin 到底是怎么把一个子命令跑起来的”。
> 关键源码：`tools/src/main/java/org/apache/rocketmq/tools/command/MQAdminStartup.java`、`SubCommand.java`
> 阅读建议：这篇重点看命令框架，不要先跳到具体某个命令功能。

## 先给结论

- RocketMQ 的管理命令体系是 `MQAdminStartup + SubCommand` 的组合。
- `MQAdminStartup` 负责命令注册、参数解析和入口分发。
- `SubCommand` 负责单个命令的名称、说明、参数和执行。
- 所以运维命令是一个插件化的子命令框架，不是一堆散乱 main 方法。

## `SubCommand` 的职责

一个子命令必须提供：

- `commandName()`
- `commandDesc()`
- `buildCommandlineOptions(...)`
- `execute(...)`

这说明每个命令都在同一个约束下工作：

- 定义名字
- 定义参数
- 执行逻辑

## `MQAdminStartup` 做了什么

它主要负责：

1. [[69-mqadmin-CLI架构]]设置 remoting 版本
2. [[65-ACL认证授权]] [[65-ACL认证授权]]初始化所有子命令
3. 解析命令行
4. 找到对应子命令
5. 执行命令

这说明 `mqadmin` 是一个统一入口，而不是每个命令独立启动。

## 为什么它要统一注册

因为这样可以：

- 统一帮助输出
- 统一 ACL hook
- 统一 namesrv 参数
- 统一版本处理

所以它是一个标准的命令调度框架。

## 运维命令覆盖了什么

`MQAdminStartup` 里能看到很多类别：

- topic
- consumer group
- broker
- controller
- ha
- lite
- auth
- metrics
- queue
- offset

这说明 mqadmin 已经不只是“查消息工具”，而是完整运维控制面入口。

## ACL 和命令行的关系

如果没有传入 RPC hook，`MQAdminStartup` 会默认加载 ACL 配置文件。

所以运维命令不是无认证直连，而是：

- 也纳入 ACL 体系

## 哪些旧理解容易错

### “mqadmin 只是查询工具”

不对。它覆盖配置、治理、控制和排障。

### “每个命令都是独立程序”

不对。它们共享入口和框架。

### “运维命令不走 ACL”

不对。默认会走 ACL hook。

## 这篇最值得记住的点

- `MQAdminStartup` 是统一命令入口。
- `SubCommand` 定义了统一命令契约。
- 运维命令是 RocketMQ 控制面的重要组成部分。

## 建议连读

1. [[69-mqadmin-CLI架构]]
2. [[65-ACL认证授权]]