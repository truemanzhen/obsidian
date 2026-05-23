# ConfigurationManager - 配置加载体系

> 研究定位：只看 Proxy 的配置从哪来、怎么落到内存、谁在用它。
> 关键源码：`proxy/src/main/java/org/apache/rocketmq/proxy/config/ConfigurationManager.java`、`proxy/src/main/java/org/apache/rocketmq/proxy/config/Configuration.java`、`proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java`
> 关联测试：`proxy/src/test/java/org/apache/rocketmq/proxy/config/ConfigurationManagerTest.java`

## 先给结论

- `ConfigurationManager` 是全局静态访问点。
- 真正保存数据的是 `Configuration`，里面用 `AtomicReference` 持有 `ProxyConfig` 和 `AuthConfig`。
- 配置文件默认是 `rmq-proxy.json`。
- 读取优先级是：系统属性指定路径 > `RMQ_PROXY_HOME/conf/rmq-proxy.json` > 测试资源 > `./` 兜底。

## 源码骨架

```java
public class ConfigurationManager {
    public static final String RMQ_PROXY_HOME = "RMQ_PROXY_HOME";
    protected static String proxyHome;
    protected static Configuration configuration;

    public static void initEnv() {
        proxyHome = System.getenv(RMQ_PROXY_HOME);
        if (StringUtils.isEmpty(proxyHome)) {
            proxyHome = System.getProperty(RMQ_PROXY_HOME, DEFAULT_RMQ_PROXY_HOME);
        }
        if (proxyHome == null) {
            proxyHome = "./";
        }
    }

    public static void initConfig() throws Exception {
        configuration = new Configuration();
        configuration.init();
    }
}
```

## 配置加载链

`Configuration.init()` 做了两件事：

1. 把 JSON 反序列化成 `ProxyConfig`
2. 再把同一份 JSON 反序列化成 `AuthConfig`

然后它会把：

- `authConfig.setConfigName(proxyConfig.getProxyName())`
- `authConfig.setClusterName(proxyConfig.getRocketMQClusterName())`

也就是说，Proxy 配置和鉴权配置是同一个配置文件的两个视图。

## 读取顺序

### 1. 系统属性

如果设置了 `com.rocketmq.proxy.configPath`，直接读这个文件。

### 2. 测试资源

单测环境下会优先尝试 classpath 里的 `rmq-proxy-home/conf/rmq-proxy.json`。

### 3. 正式文件

最终会回落到：

`<proxyHome>/conf/rmq-proxy.json`

## 关键对象

### `Configuration`

- 持有 `ProxyConfig`
- 持有 `AuthConfig`
- 用 `AtomicReference` 做可见性控制

### `ProxyConfig`

- Proxy 的所有运行时配置都在这里
- 默认 `proxyMode` 是 `CLUSTER`
- 默认 `grpcServerPort` 是 `8081`

### `AuthConfig`

- 鉴权开关、AK/SK、授权链都从同一个 JSON 文件来

## 扩展知识

- `ConfigurationManager` 本身不是配置中心，它只是启动期和运行期的全局读取口。
- 多数配置并不会自动热更新，只有少数文件监听链路会触发运行时重载，例如 TLS 证书。
- `ProxyConfig` 的默认路径依赖 `ConfigurationManager.getProxyHome()`，所以 `initEnv()` 必须先执行。

## 这篇最该记住的点

- 配置不是分散在多个文件里，而是以一个 JSON 文件为中心。
- `ConfigurationManager` 只是门面，`Configuration` 才是真正的状态容器。
- `formatProxyConfig()` 只是为了日志可读性，不影响配置语义。

## 关联阅读

- [[14-ProxyStartup]]
- [[15-ProxyMode]]
- [[18-GrpcServer]]
