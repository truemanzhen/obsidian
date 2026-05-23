# ProxyMode - Local/Cluster 双模式

> 研究定位：只回答一件事，Proxy 到底以什么模式启动、模式值在哪里被消费、它和 `ProxyStartup` 的关系是什么。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/ProxyMode.java`、`proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`、`proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java`

## 先给结论

- `ProxyMode` 只是运行模式枚举，不负责启动逻辑。
- 5.5.0 只区分 `LOCAL` 和 `CLUSTER` 两种模式。
- `ProxyConfig` 默认值是 `CLUSTER`，所以不显式改配置时会按集群模式起。
- 真正的分支点在 `ProxyStartup.createMessagingProcessor()`，不是在 `ProxyMode` 本身。

## 源码骨架

```java
public enum ProxyMode {
    LOCAL("LOCAL"),
    CLUSTER("CLUSTER");

    public static boolean isClusterMode(String mode) {
        if (mode == null) {
            return false;
        }
        return CLUSTER.mode.equals(mode.toUpperCase());
    }

    public static boolean isLocalMode(String mode) {
        if (mode == null) {
            return false;
        }
        return LOCAL.mode.equals(mode.toUpperCase());
    }
}
```

## 模式含义

### `LOCAL`

- Proxy 和 Broker 同 JVM 启动。
- 典型用途是本地调试、学习、单机联调。
- 入口里会走 `BrokerStartup.createBrokerController(...)`。

### `CLUSTER`

- Proxy 只负责接入和转发。
- Broker 独立部署，Proxy 通过 `namesrvAddr` 找路由。
- 入口里会走 `DefaultMessagingProcessor.createForClusterMode()`。

## 真实调用链

`ProxyStartup` 里对模式的消费顺序是：

1. `parseCommandLineArgument()`
2. `initConfiguration()`
3. `ConfigurationManager.getProxyConfig().getProxyMode()`
4. `ProxyMode.isClusterMode(...)` / `ProxyMode.isLocalMode(...)`
5. 创建对应的 `MessagingProcessor`

所以模式不是“随时切换”的开关，而是启动前决策。

## 扩展知识

- 这里用字符串判定而不是直接比较枚举对象，是为了兼容配置文件和命令行输入。
- 代码里同时提供了 `String` 和 `ProxyMode` 两套重载，避免上层重复 `toUpperCase()`。
- 模式切换通常意味着 Broker 拓扑变化，应该按重启处理，而不是热切换。

## 关联阅读

- [[14-ProxyStartup]]
- [[16-ConfigurationManager]]