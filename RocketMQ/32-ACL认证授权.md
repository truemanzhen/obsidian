# ACL 认证授权

> 源码位置：`auth/` + `broker/src/main/java/org/apache/rocketmq/broker/auth/`

## 🏗️ 整体架构

5.5.0 重构了整套认证授权体系，从原来的简单 ACL 升级为 **Authentication（认证）+ Authorization（授权）** 双层架构。

```
┌─────────────────────────────────────────────────────────┐
│                    请求进入                              │
├─────────────────────────────────────────────────────────┤
│              Authentication（认证层）                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │  "你是谁？" — 验证身份                            │  │
│  │  ┌──────────┬──────────┬────────────────────┐   │  │
│  │  │Strategy  │Evaluator │MetadataManager     │   │  │
│  │  └──────────┴──────────┴────────────────────┘   │  │
│  └──────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────┤
│              Authorization（授权层）                      │
│  ┌──────────────────────────────────────────────────┐  │
│  │  "你能做什么？" — 检查权限                         │  │
│  │  ┌──────────┬──────────┬────────────────────┐   │  │
│  │  │Strategy  │Evaluator │MetadataManager     │   │  │
│  │  └──────────┴──────────┴────────────────────┘   │  │
│  └──────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────┤
│              业务处理                                    │
└─────────────────────────────────────────────────────────┘
```

## 🔐 Authentication（认证层）

### 核心类

| 类 | 职责 |
|---|---|
| `AuthenticationStrategy` | 认证策略接口 |
| `StatelessAuthenticationStrategy` | 无状态认证（AK/SK 签名验证） |
| `StatefulAuthenticationStrategy` | 有状态认证（Session/Token） |
| `AuthenticationEvaluator` | 认证执行器，协调 Strategy 和 Metadata |
| `AuthenticationMetadataManager` | 认证元数据管理（用户信息存储） |
| `AuthenticationProvider` | 认证提供者接口 |
| `AuthenticationContext` | 认证上下文（请求信息封装） |
| `Subject` / `User` | 认证主体（用户实体） |

### 认证流程

```
1. 请求到达（携带 AK + 签名）
   ↓
2. AuthenticationEvaluator.evaluate(context)
   ├── 从请求中提取 AK（Access Key）
   ├── 通过 MetadataManager 查询用户信息
   │   └── 检查 UserStatus（是否启用/禁用）
   ├── 调用 Strategy.authenticate(context, subject)
   │   ├── StatelessAuthenticationStrategy：
   │   │   ├── 使用 SK 计算 HMAC 签名
   │   │   └── 对比请求中的签名
   │   └── StatefulAuthenticationStrategy：
   │       └── 验证 Session/Token 有效性
   └── 认证失败 → 抛出 AuthenticationException
```

### AK/SK 签名算法

```java
// 无状态认证
public class StatelessAuthenticationStrategy extends AbstractAuthenticationStrategy {
    @Override
    public void authenticate(AuthenticationContext context, Subject subject) {
        String accessKey = context.getAccessKey();
        String signature = context.getSignature();
        String secretKey = subject.getSecretKey();

        // HMAC-SHA256 签名
        String computedSignature = computeHmacSha256(secretKey, context.getCanonicalRequest());

        if (!signature.equals(computedSignature)) {
            throw new AuthenticationException("Signature mismatch");
        }
    }
}
```

### 用户模型

```java
public class Subject {
    private String accessKey;      // 访问密钥
    private String secretKey;      // 私密密钥
    private SubjectType type;      // USER / APPLICATION
}

public class User {
    private String username;
    private String accessKey;
    private String secretKey;
    private UserType type;          // ROOT / NORMAL
    private UserStatus status;      // ENABLED / DISABLED
}
```

## 🛡️ Authorization（授权层）

### 核心类

| 类 | 职责 |
|---|---|
| `AuthorizationStrategy` | 授权策略接口 |
| `AclAuthorizationHandler` | ACL 授权处理器 |
| `AuthorizationEvaluator` | 授权执行器 |
| `AuthorizationMetadataManager` | 授权元数据管理（策略存储） |
| `Policy` / `PolicyEntry` | 授权策略实体 |
| `Resource` | 资源标识（Topic/Group） |
| `Environment` | 环境信息（IP/时间等） |
| `Decision` | 授权决策结果 |

### 授权流程

```
1. 认证通过后，进入授权检查
   ↓
2. AuthorizationEvaluator.evaluate(context)
   ├── 构建 RequestContext（资源、操作、环境）
   ├── 通过 MetadataManager 查询用户策略
   │   └── 获取 Policy 列表
   ├── 调用 Strategy.authorize(context, policies)
   │   ├── 遍历 Policy 列表
   │   ├── 匹配 Resource（Topic/Group）
   │   ├── 检查操作权限（SEND / SUBSCRIBE / ADMIN）
   │   └── 检查环境条件（IP 白名单/黑名单）
   └── 返回 Decision
       ├── ALLOW → 允许访问
       └── DENY → 拒绝访问
```

### 策略模型

```java
public class Policy {
    private String subject;        // 授权主体（AccessKey）
    private List<PolicyEntry> entries;  // 策略条目列表
}

public class PolicyEntry {
    private Resource resource;     // 资源标识
    private Set<String> actions;   // 操作集合（SEND/SUBSCRIBE/ADMIN）
    private Decision decision;     // ALLOW / DENY
    private Environment environment; // 环境条件
}

public class Resource {
    private String topic;          // Topic 名称
    private String group;          // 消费组名称
    private String cluster;        // 集群名称
}

public class Environment {
    private Set<String> ipWhitelist;  // IP 白名单
    private Set<String> ipBlacklist;  // IP 黑名单
}
```

### 操作权限类型

| 操作 | 说明 |
|---|---|
| `SEND` | 发送消息 |
| `SUBSCRIBE` | 订阅消费 |
| `ADMIN` | 管理操作 |

## 🔌 Proxy 集成

### 拦截器注入

```java
// Proxy 的 AuthenticationPipeline 和 AuthorizationPipeline
public class AuthenticationPipeline implements RequestPipeline {
    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand cmd) {
        // 从请求中提取认证信息
        AuthenticationContext context = buildAuthContext(cmd);
        // 执行认证
        authenticationEvaluator.evaluate(context);
    }
}

public class AuthorizationPipeline implements RequestPipeline {
    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand cmd) {
        // 构建授权上下文
        AuthorizationContext context = buildAuthzContext(cmd);
        // 执行授权
        authorizationEvaluator.evaluate(context);
    }
}
```

### Remoting Hook 集成

```java
// 在 Broker 侧通过 RPCHook 集成
public class AuthenticationHook implements RPCHook {
    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        // 提取 AK/SK
        // 执行认证
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {
        // 记录审计日志
    }
}
```

## 🔄 元数据存储

### 本地模式（LocalAuthenticationMetadataProvider）

```java
public class LocalAuthenticationMetadataProvider implements AuthenticationMetadataProvider {
    // 用户信息存储在本地文件
    // 路径：${storePath}/config/acl/
}
```

### 配置文件格式

```json
{
  "users": [
    {
      "accessKey": "RocketMQ",
      "secretKey": "12345678",
      "whiteIpList": ["192.168.1.*"],
      "admin": true
    },
    {
      "accessKey": "producer1",
      "secretKey": "abcdefg",
      "whiteIpList": [],
      "admin": false
    }
  ],
  "globalWhiteRemoteAddresses": ["127.0.0.1"]
}
```

## 🔗 与旧版 ACL 的兼容

5.5.0 提供了迁移工具（`auth/migration/`）：

```java
// 旧版 PlainAccessConfig → 新版 Policy
public class AuthMigrator {
    public Policy migrate(PlainAccessConfig oldConfig) {
        Policy policy = new Policy();
        policy.setSubject(oldConfig.getAccessKey());

        // 转换 Topic 权限
        for (TopicPermission perm : oldConfig.getTopicPerms()) {
            PolicyEntry entry = new PolicyEntry();
            entry.setResource(new Resource(perm.getTopic()));
            entry.setActions(parseActions(perm.getPerm()));
            policy.getEntries().add(entry);
        }

        return policy;
    }
}
```

## 💡 设计亮点

1. **双层架构**：认证（你是谁）和授权（你能做什么）分离
2. **策略模式**：支持无状态（AK/SK）和有状态（Token）两种认证方式
3. **Evaluator 协调器**：统一编排 Strategy 和 Metadata，职责清晰
4. **环境感知**：授权支持 IP 白名单/黑名单、时间窗口等条件
5. **向后兼容**：提供迁移工具从旧版 ACL 升级

## ⚠️ 安全注意事项

1. **SK 不要硬编码**：通过环境变量或配置中心管理
2. **IP 白名单谨慎配置**：避免 `*` 通配
3. **定期轮换 AK/SK**：防止泄露后被滥用
4. **审计日志**：记录所有认证/授权失败事件
5. **最小权限原则**：生产者只给 SEND 权限，消费者只给 SUBSCRIBE 权限

## 🔗 与其他模块的关系

- **Remoting**：通过 RPCHook 在请求前后执行认证授权
- **Proxy**：通过 Pipeline 模式集成 AuthenticationPipeline / AuthorizationPipeline
- **Broker**：AdminBrokerProcessor 中管理 ACL 配置
- **Client**：客户端携带 AK/SK 签名发送请求

---

#核心类 #面试高频 #设计模式
