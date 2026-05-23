# 延迟消息 Store 层

> 研究定位：把 5.5.0 的 timer 体系从“调度功能”提升到“独立存储子系统”层面，回答 Timer 为什么要有自己的 log、wheel 和 checkpoint。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/timer/TimerMessageStore.java`、`TimerLog.java`、`TimerWheel.java`、`TimerCheckpoint.java`
> 阅读建议：这篇和 [[34-延迟消息设计]] 连读，前者看语义，后者看落地。

## 先给结论

- Timer 存储不是把消息简单挂一个延迟时间，而是建立了独立的落盘结构。
- `TimerMessageStore` 是主 orchestrator，内部同时管理 wheel、log、checkpoint 和多条后台线程。
- 5.5.0 的 timer 体系可以看作“精度更高、状态更多、恢复更复杂”的延迟消息实现。

## 核心对象

### `TimerLog`

负责顺序追加 timer 记录。

它的单位不是普通 CommitLog 消息，而是 timer 专用记录单元。

### `TimerWheel`

负责按时间槽组织待投递消息。

它解决的是：

- 到期消息的调度组织

### `TimerCheckpoint`

负责保存恢复所需的关键进度。

这决定了：

- broker 重启后能从哪里继续恢复 timer 状态

### `TimerMessageStore`

这是总控类。

它管理：

- enqueue 队列
- dequeue 队列
- flush
- warm
- metrics
- 与 MessageStore 的联动

## TimerLog 的关键结构

`TimerLog` 不是简单文件包装，它有专门的记录布局：

- size
- prev pos
- magic value
- current write time
- delayed time
- offsetPy
- sizePy
- real topic hash
- reserved

这说明 TimerLog 记录的是：

- 延迟投递所需的调度元数据

而不只是“原消息体”。

## TimerWheel 的角色

TimerWheel 的目标是：

- 按时间把消息放进对应轮槽
- 再在到期后取出投递

所以它更像一个调度索引，而不是消息本体存储。

## `TimerMessageStore` 的后台链

它内部有多条服务线程：

- `TimerEnqueueGetService`
- `TimerEnqueuePutService`
- `TimerDequeueWarmService`
- `TimerDequeueGetService`
- `TimerDequeuePutMessageService`
- `TimerDequeueGetMessageService`
- `TimerFlushService`

这说明 timer 子系统不是一个循环任务，而是一套完整流水线。

## 为什么 timer 子系统必须单独存在

因为它要同时满足：

- 精确投递时间
- 恢复后继续投递
- 与普通 CommitLog 消费窗口隔离
- 维持自己的 checkpoint 和 metrics

如果全部塞回传统 delay level，会非常难扩展。

## 与普通 CommitLog 的关系

Timer 体系最终仍然要借助 RocketMQ 的消息存储基础设施，但它并不等同于普通消费链。

可以这样理解：

- 普通消息：CommitLog -> ConsumeQueue
- Timer 消息：TimerLog / TimerWheel -> 到期后再转回消息主链

所以它是“先延后，再恢复主链”的模式。

## 5.5.0 的关键边界

- Timer 不是纯语义层概念，而是独立存储系统。
- 只有理解 TimerLog 和 TimerWheel，才能解释为什么它比经典 delay level 更复杂。
- checkpoint 是恢复边界，不是可有可无的附件。

## 这篇最值得记住的点

- TimerMessageStore 是 timer 子系统总控。
- TimerLog 负责记录，TimerWheel 负责调度，TimerCheckpoint 负责恢复。
- 它和经典 delay level 是不同层次的实现。

## 和前一篇的关系

-  讲的是语义和双路径。
- 本文讲的是 timer 路径如何在 store 层真正落地。

## 建议连读

1. [[34-延迟消息设计]]
2. [[28-CommitLog写入流程]]