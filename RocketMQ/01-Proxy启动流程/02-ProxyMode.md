# ProxyMode — Local/Cluster 双模式

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/ProxyMode.java`
> 🏷️ #设计模式 #核心类

## 🎯 类的职责

用枚举定义 Proxy 的两种运行模式，并提供模式判断工具方法。

## 🔍 源码逐行

```java
public enum ProxyMode {
    LOCAL("LOCAL"),     // 本地模式：内嵌 Broker
    CLUSTER("CLUSTER"); // 集群模式：纯 Proxy，连接外部 Broker

    private final String mode;

    // 构造器
    ProxyMode(String mode) {
        this.mode = mode;
    }

    // 判断是否为集群模式（支持 null 安全）
    public static boolean isClusterMode(String mode) {
        if (mode == null) return false;
        return CLUSTER.mode.equals(mode.toUpperCase());
    }

    // 判断是否为本地模式
    public static boolean isLocalMode(String mode) {
        if (mode == null) return false;
        return LOCAL.mode.equals(mode.toUpperCase());
    }
}
```

## 📐 两种模式的架构对比

```
┌─────────────────────────────────────────────────────┐
│                   Local 模式                         │
│                                                     │
│  ┌────────┐    ┌──────────────────────────────┐    │
│  │ Client │───→│ Proxy + Broker (同一 JVM)     │    │
│  └────────┘    └──────────────────────────────┘    │
│                                                     │
│  特点：简单、快速启动、适合开发测试                     │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│                   Cluster 模式                       │
│                                                     │
│  ┌────────┐    ┌────────┐    ┌────────┐            │
│  │ Client │───→│ Proxy  │───→│ Broker │            │
│  └────────┘    └────────┘    └────────┘            │
│                     │                               │
│                     ▼                               │
│               ┌──────────┐                          │
│               │ NameServer│                         │
│               └──────────┘                          │
│                                                     │
│  特点：Proxy 无状态、可水平扩展、生产推荐              │
└─────────────────────────────────────────────────────┘
```

## 💡 设计思考

### 为什么需要两种模式？

| 维度 | Local 模式 | Cluster 模式 |
|------|-----------|-------------|
| 部署复杂度 | 低（单进程） | 高（多组件） |
| 启动速度 | 快 | 较慢 |
| 可扩展性 | 差（单机） | 强（Proxy 无状态） |
| 适用场景 | 开发、测试、Demo | 生产环境 |

> [!tip] 实用价值
> Local 模式让开发者可以**一个 JVM 跑通整个 Proxy**，不需要额外部署 Broker 和 NameServer。
> 这种"开发模式/生产模式"的双模设计在很多中间件中都有应用（如 Spring Boot 的 devtools）。

### 模式判断的编码技巧

```java
// 用 upperCase() 忽略大小写，对用户更友好
return CLUSTER.mode.equals(mode.toUpperCase());
```

> [!note] #源码技巧
> 判断时用**枚举的字段值**做 `equals`，而不是 `mode.equals(CLUSTER.name())`。
> 这样枚举名和字段值可以不同，更灵活。

---

> ⏭️ 下一篇：[[03-ConfigurationManager]] — 配置加载体系
