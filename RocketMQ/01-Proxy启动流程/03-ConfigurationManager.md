# ConfigurationManager — 配置加载体系

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/config/ConfigurationManager.java`
> 🏷️ #核心类 #设计模式

## 🎯 类的职责

全局配置管理器，采用**静态单例**模式管理 Proxy 的所有配置。

## 🔍 源码逐行

```java
public class ConfigurationManager {
    // 环境变量名
    public static final String RMQ_PROXY_HOME = "RMQ_PROXY_HOME";
    // 默认 Proxy 主目录
    protected static final String DEFAULT_RMQ_PROXY_HOME = MixAll.ROCKETMQ_HOME_DIR;

    // 静态字段：全局唯一
    protected static String proxyHome;
    protected static Configuration configuration;

    // ① 初始化环境：先读环境变量，再读系统属性，最后兜底 "./"
    public static void initEnv() {
        proxyHome = System.getenv(RMQ_PROXY_HOME);        // 环境变量优先
        if (StringUtils.isEmpty(proxyHome)) {
            proxyHome = System.getProperty(RMQ_PROXY_HOME, DEFAULT_RMQ_PROXY_HOME);
        }
        if (proxyHome == null) {
            proxyHome = "./";
        }
    }

    // ② 初始化配置：创建 Configuration 对象并加载
    public static void initConfig() throws Exception {
        configuration = new Configuration();
        configuration.init();
    }

    // ③ 获取配置（全局访问点）
    public static ProxyConfig getProxyConfig() {
        return configuration.getProxyConfig();
    }

    public static AuthConfig getAuthConfig() {
        return configuration.getAuthConfig();
    }
}
```

## 📐 配置加载流程

```
main()
  │
  ├─ parseCommandLineArgument()    解析 -pc / -pm 等参数
  │
  └─ initConfiguration()
       │
       ├─ ConfigurationManager.initEnv()
       │    │
       │    ├─ 1. System.getenv("RMQ_PROXY_HOME")     ← 环境变量
       │    ├─ 2. System.getProperty("RMQ_PROXY_HOME") ← JVM 系统属性
       │    └─ 3. 兜底 "./"                             ← 默认值
       │
       └─ ConfigurationManager.initConfig()
            │
            ├─ new Configuration()
            │    └─ 加载 proxy 配置文件 (proxy.conf)
            │    └─ 加载认证配置 (auth.conf)
            │
            └─ configuration.init()
                 └─ 合并命令行参数覆盖文件配置
```

## 💡 设计思考

### 1. 配置优先级（从高到低）

```
命令行参数 > JVM 系统属性 > 配置文件 > 默认值
```

> [!important] 这是 RocketMQ 的通用配置优先级模式
> 在 Broker、NameServer 中也遵循同样的优先级规则。

### 2. 静态单例 vs 依赖注入

```java
// RocketMQ 的做法：静态工具类
ConfigurationManager.getProxyConfig()

// 如果用 Spring：@Autowired ProxyConfig proxyConfig
```

> [!note] 为什么不用 Spring？
> RocketMQ 作为基础中间件，追求**零外部依赖**（除了 gRPC、Netty）。
> 静态单例虽然不利于测试，但部署简单、启动快、无容器依赖。
> 这是**中间件设计的典型取舍**。

### 3. 配置热更新

> `ProxyConfig` 中的某些配置支持运行时修改（如 `grpcShutdownTimeSeconds`），
> 但大部分配置修改后需要重启。这是常见的"热更新 + 冷重启"混合策略。

---

## 🧩 相关类

| 类 | 职责 |
|----|------|
| `Configuration` | 配置容器，持有 `ProxyConfig` 和 `AuthConfig` |
| `ProxyConfig` | 所有 Proxy 配置项的 POJO |
| `ConfigurationManager` | 全局访问点，管理 Configuration 生命周期 |

---

> ⏭️ 下一篇：[[04-GrpcServerBuilder]] — gRPC 服务器构建
