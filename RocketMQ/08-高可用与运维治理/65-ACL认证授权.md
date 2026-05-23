# ACL 认证授权

> 研究定位：把 RocketMQ 的 ACL 请求签名、权限校验和工具链入口拆开，回答“客户端到底是怎么带签名访问 Broker 的”。
> 关键源码：`client/src/main/java/org/apache/rocketmq/acl/common/AclClientRPCHook.java`、`auth/` 下的权限管理实现
> 阅读建议：这篇重点看请求前签名和服务端校验的关系，不要把它和业务 topic 权限混成一类。

## 先给结论

- ACL 是 RocketMQ 的请求级安全机制，不是 topic 级的简单黑白名单。
- 客户端通过 `AclClientRPCHook` 在请求发送前补签名信息。
- 签名计算依赖请求 extFields 的排序和组合。
- 认证授权逻辑在 RocketMQ 里是一个独立子系统，不是 remoting 层顺手加的一个判断。

## 客户端签名怎么加上去

`AclClientRPCHook.doBeforeRequest(...)` 的核心动作是：

1. [[68-Admin工具与运维命令]]写入 `ACCESS_KEY`
2. [[69-mqadmin-CLI架构]] [[69-mqadmin-CLI架构]]可选写入 `SECURITY_TOKEN`
3. 按 extFields 组合请求体
4. 用 secretKey 计算签名
5. 写回 `SIGNATURE`

所以请求签名不是“附加头”，而是：

- 请求可验证身份的一部分

## 为什么要先排序 extFields

`parseRequestContent(...)` 会把 extFields 转成 `TreeMap`。

这样做是为了：

- 保证签名输入稳定
- 避免同一请求因 map 顺序不同导致签名不一致

这也是 ACL 里最容易被忽略但最关键的一点。

## ACL 解决的是什么问题

它解决的是：

- 这次请求是谁发的
- 这个请求是否被允许

所以它不是 message filter，也不是 topic subscription。

## 认证授权和权限模型

RocketMQ 的 ACL 往往还会配合：

- 用户
- 权限集合
- secret / access key

形成完整访问控制链。

这说明 ACL 的对象不是消息本身，而是：

- 请求方身份与资源访问权限

## 哪些旧理解容易错

### “ACL 只是加个 token”

不对。它包含签名计算和权限校验。

### “ACL 只在 broker 端处理”

不对。客户端先参与签名，服务端再验证。

### “ACL 跟 topic filter 是一回事”

不对。它们关注的问题完全不同。

## 这篇最值得记住的点

- ACL 是请求级安全机制。
- `AclClientRPCHook` 是客户端签名入口。
- extFields 顺序化后再签名是关键细节。

## 和后续笔记的关系

-  会看到工具链如何复用 ACL 配置。
-  会继续看命令入口如何解析和执行。

## 建议连读

1. [[68-Admin工具与运维命令]]
2. [[69-mqadmin-CLI架构]]