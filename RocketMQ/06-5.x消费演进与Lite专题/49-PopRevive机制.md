# PopRevive 机制

> 研究定位：把 POP 的“超时后如何重投”从概念描述拆到 revive log、CK/ACK 对账、重写检查点和 retry 重投闭环。
> 关键源码：`broker/.../PopReviveService.java`、`broker/.../AckMessageProcessor.java`、`broker/.../ChangeInvisibleTimeProcessor.java`、`broker/.../PopMessageProcessor.java`、`store/.../pop/PopCheckPoint.java`
> 阅读建议：先看  建立 POP 总体语义，再回到本篇看 revive 这条后台补偿链。

## 先给结论

- PopRevive 不是“客户端超时后自己再拉一次”，而是 Broker 后台线程围绕 revive topic 做的一条补偿扫描链。
- revive topic 里不只存 `PopCheckPoint`，也存 `ACK` / `BATCH_ACK` 事件；`PopReviveService` 会把它们先对账，再决定是否重投。
- 超时后的重投目标通常不是原 topic，而是 POP retry topic；对应代码在 `PopReviveService.reviveRetry(...)` 里显式调用 `KeyBuilder.buildPopRetryTopic(...)`。
- `PopCheckPoint` 不是只靠连续 offset 工作。5.5.0 里它已经支持 `queueOffsetDiff`，所以一个 CK 可以表示非连续的多条消息。
- `ChangeInvisibleTimeProcessor` 并不是“改一下内存里的不可见时间”，它会写新的 CK，并可能补写 ACK 到 revive topic。

## revive log 到底是什么

POP 相关组件共享一个 cluster 级 revive topic：

```java
public static final String REVIVE_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "REVIVE_LOG_";

public static String buildClusterReviveTopic(String clusterName) {
    return PopAckConstants.REVIVE_TOPIC + clusterName;
}
```

对应使用点：

- `PopMessageProcessor` 写 CK
- `AckMessageProcessor` 写 ACK / BATCH_ACK
- `ChangeInvisibleTimeProcessor` 写续期后的 CK 与 ACK
- `PopReviveService` 作为消费者扫描 `REVIVE_LOG_<cluster>`

所以 revive topic 不是“重试消息主题”，而是 POP 内部的状态日志。

## PopCheckPoint 真正承载了什么

`PopCheckPoint` 的关键字段不是只有 `startOffset + invisibleTime`：

```java
private long startOffset;
private long popTime;
private long invisibleTime;
private int bitMap;
private byte num;
private int queueId;
private String topic;
private String cid;
private long reviveOffset;
private List<Integer> queueOffsetDiff;
private String brokerName;
private String rePutTimes;
private boolean suspend;
```

这几个字段对应的运行时含义：

- `topic` / `cid` / `queueId`：这批消息属于哪个 topic、group、queue。
- `popTime + invisibleTime`：决定 `getReviveTime()`，也就是理论上的超时点。
- `bitMap`：标记这批消息哪些 offset 已经被 ACK。
- `num`：这次 CK 覆盖多少条消息。
- `queueOffsetDiff`：支持非连续 offset，不再强依赖 `startOffset + i`。
- `rePutTimes`：CK 被重写过多少次。
- `suspend`：用于区分某些续期 / nack 场景是否递增 `reconsumeTimes`。

最关键的两个方法：

```java
public int indexOfAck(long ackOffset) { ... }

public long ackOffsetByIndex(byte index) { ... }
```

这说明 revive 处理的本质不是“拿一批 offset 做顺序遍历”，而是“用 CK 恢复出这一批业务消息的 offset 集合，再按 ACK 位图补齐未确认项”。

## revive 不是直接扫业务 topic

`PopReviveService` 的真实主线是：

```text
扫描 revive topic
  -> 读出 CK / ACK / BATCH_ACK
  -> 先把 ACK 合并回对应 CK
  -> 找出已到 reviveTime 但仍未 ACK 的 offset
  -> 读业务消息
  -> 重投到 POP retry topic
```

这条线里最容易被忽略的是“先对账后重投”。

源码里 `consumeReviveMessage(...)` 会：

- 把 `CK_TAG` 解析成 `PopCheckPoint`
- 把 `ACK_TAG` / `BATCH_ACK_TAG` 回填到 `bitMap`
- 通过 `mergeKey = topic + group + queueId + startOffset + popTime + brokerName` 把 ACK 归并回正确 CK

这意味着：

- revive topic 本身就是一条事件流
- PopReviveService 不是只看 CK 不看 ACK
- 如果只把它理解成“定时扫描超时消息”，会漏掉最关键的对账过程

## ACK 与续期为什么也要写 revive topic

`AckMessageProcessor` 在 `PopBufferMergeService` 无法直接吃掉 ACK 时，会补写一条 ACK 事件：

```java
msgInner.setTopic(reviveTopic);
msgInner.setTags(PopAckConstants.ACK_TAG);
msgInner.setDeliverTimeMs(popTime + invisibleTime);
```

批量 ACK 则写 `BATCH_ACK_TAG`。

`ChangeInvisibleTimeProcessor` 做续期时也不是只改内存，它会：

- 先构造新的 `PopCheckPoint`
- 写入 revive topic
- 再通过 `ackOrigin(...)` 补写原 CK 对应的 ACK

因此 revive log 里同时存在：

- 原始 CK
- 后续 ACK / BATCH_ACK
- 续期之后的新 CK

这就是为什么 revive 能在后台线程里把整个 POP 生命周期重新拼出来。

## 超时后到底投到哪里

`PopReviveService.reviveRetry(...)` 里写得很直接：

```java
if (!popCheckPoint.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
    msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getTopic(), popCheckPoint.getCId(),
        brokerController.getBrokerConfig().isEnableRetryTopicV2()));
} else {
    msgInner.setTopic(popCheckPoint.getTopic());
}
```

这说明：

- 正常 topic 上 pop 出来的消息，超时后会进 POP retry topic。
- 如果当前 CK 本来就来自 retry topic，则继续留在 retry topic 上递进。

它不是简单“重新放回原 topic”。

同时它还会写入：

- `PROPERTY_FIRST_POP_TIME`
- `PROPERTY_ORIGIN_GROUP`

并在非 `suspend` 场景下递增 `reconsumeTimes`。

## CK 重写不是异常路径，而是机制本体

当 revive 某条消息失败，`PopReviveService` 不会立刻丢掉 CK，而是调用 `rePutCK(...)` 重写一个更小粒度的新 CK：

```java
newCk.setNum((byte) 1);
newCk.setStartOffset(pair.getObject1());
newCk.setRePutTimes(String.valueOf(rePutTimes + 1));
```

并按 `ckRewriteIntervalsInSeconds` 递增 invisible time。

这段逻辑非常关键，因为它说明：

- 一次 POP 批量拿到的 N 条消息，超时补偿时可以被拆成更细粒度的 CK。
- CK 重写是 revive 的正常补偿手段，不是单纯的失败重试日志。
- `rePutTimes` 不是统计字段，而是影响下一次补偿节奏的控制变量。

## `PopBufferMergeService` 的角色不是可有可无

很多资料会直接把 POP 讲成：

`CK -> ACK -> Revive`

但 5.5.0 中间还夹着一层 `PopBufferMergeService`。

它的作用是：

- 优先在内存里吸收 ACK
- 尽量避免每个 ACK 都落一条 revive log
- 给 `AckMessageProcessor` / `ChangeInvisibleTimeProcessor` 提供快速命中

所以更准确的理解是：

`CK -> ACK 先尝试内存合并 -> 必要时落 revive log -> 后台 revive 对账`

## 这篇要纠正的几个旧说法

- “revive topic 是按 clientId 单独派生的。”  
  5.5.0 源码里是 cluster 级 `REVIVE_LOG_<cluster>`。

- “CK 只能表示连续 offset。”  
  5.5.0 已有 `queueOffsetDiff`，支持非连续映射。

- “ChangeInvisibleTime 只是把 invisibleTime 改大。”  
  实现上它会写新 CK，并可能补 ACK，仍然经过 revive log。

- “超时后直接重新投到原 topic。”  
  默认是进入 POP retry topic，然后由 POP 消费链继续处理。

## 研究时值得继续核对的问题

- `brokerConfig.isEnableRetryTopicV2()` 打开时，POP V1/V2 retry topic 的切换点具体在哪几条链路上。
- `suspend=true` 的场景下，为什么 revive 不递增 `reconsumeTimes`，它对应的是哪类 nack / renew 语义。
- `PopBufferMergeService` 命中与未命中时，ACK 的落盘路径分别是什么。
- `enableSkipLongAwaitingAck` 打开后，mock CK 补偿会不会改变某些极端延迟 ACK 的恢复行为。

## 相关笔记

- [[47-长轮询机制详解]]

- [[53-5.x消费模型与负载均衡]]