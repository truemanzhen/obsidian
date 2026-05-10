# RequestPipeline — 责任链接口设计

> 📄 源码路径：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/pipeline/RequestPipeline.java`
> 🏷️ #设计模式 #面试高频

## 🎯 接口定义

```java
public interface RequestPipeline {

    // 执行管线处理
    void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request);

    // 链式组合：返回一个新的 Pipeline，先执行 source，再执行 this
    default RequestPipeline pipe(RequestPipeline source) {
        return (ctx, headers, request) -> {
            source.execute(ctx, headers, request);  // 先执行前一个
            execute(ctx, headers, request);           // 再执行自己
        };
    }
}
```

## 📐 Pipeline 组装过程

```java
// 组装代码（在 GrpcMessagingApplication.create() 中）
RequestPipeline pipeline = (context, headers, request) -> {};  // 空 Pipeline

pipeline = pipeline.pipe(new AuthorizationPipeline(...));   // ③
pipeline = pipeline.pipe(new AuthenticationPipeline(...));  // ②
pipeline = pipeline.pipe(new ContextInitPipeline());        // ①
```

**组装过程**（每调用一次 `pipe()`，包装一层）：

```
pipeline = 空Pipeline
    ↓ pipe(AuthorizationPipeline)
pipeline = [Authorization → 空]
    ↓ pipe(AuthenticationPipeline)
pipeline = [Authentication → Authorization → 空]
    ↓ pipe(ContextInitPipeline)
pipeline = [ContextInit → Authentication → Authorization → 空]
```

**执行时**：
```
请求进入
  → ContextInitPipeline.execute()      ① 初始化上下文
  → AuthenticationPipeline.execute()   ② 认证（验证身份）
  → AuthorizationPipeline.execute()    ③ 鉴权（验证权限）
```

## 💡 设计亮点

### 1. 函数式 Pipeline（区别于传统责任链）

**传统责任链**（如 Servlet Filter）：
```java
// 每个节点需要持有 next 引用
class Filter {
    Filter next;
    void doFilter(request, response) {
        // 自己的逻辑
        next.doFilter(request, response);  // 调用下一个
    }
}
```

**RocketMQ 的函数式 Pipeline**：
```java
// 不需要 next 引用，通过 lambda 闭包组合
default RequestPipeline pipe(RequestPipeline source) {
    return (ctx, headers, request) -> {
        source.execute(ctx, headers, request);
        execute(ctx, headers, request);
    };
}
```

> [!tip] 函数式 vs 传统责任链
> | 维度 | 传统责任链 | 函数式 Pipeline |
> |------|-----------|----------------|
> | 节点关系 | 每个节点持有 `next` | 无引用，lambda 组合 |
> | 执行控制 | 节点自己决定是否传递 | 自动顺序执行 |
> | 可组合性 | 需要手动链接 | `pipe()` 链式组合 |
> | 新增节点 | 修改链表结构 | 添加一行 `pipe()` |
>
> 函数式写法更简洁，且每个 Pipeline 节点是**无状态的**（不持有其他节点的引用）。

### 2. 与 Java Stream API 的相似性

```java
// Stream API
list.stream()
    .filter(...)    // 中间操作
    .map(...)       // 中间操作
    .collect(...)   // 终止操作

// RequestPipeline
pipeline
    .pipe(ContextInit)        // 上下文初始化
    .pipe(Authentication)     // 认证
    .pipe(Authorization)      // 鉴权
```

> [!note] 思想相通
> 都是**函数组合**的思想：将多个小函数组合成一个大函数。
> `pipe()` 类似 `Stream` 的中间操作，`execute()` 类似终止操作。

---

## 🧩 相关实现

| Pipeline | 职责 | 位置 |
|----------|------|------|
| `ContextInitPipeline` | 从 Metadata 初始化 ProxyContext | `pipeline/` |
| `AuthenticationPipeline` | 验证客户端身份 | `pipeline/` |
| `AuthorizationPipeline` | 验证操作权限 | `pipeline/` |

---

> ⏭️ 下一篇：[[08-Interceptor链]] — 三级拦截器体系
