# MappedFile 与内存映射

> 研究定位：回答 RocketMQ 默认存储为什么围绕 `MappedFile` / `MappedFileQueue` 组织，而不是直接把 `FileChannel` 调用散落在各条写入链上。
> 关键源码：`store/src/main/java/org/apache/rocketmq/store/logfile/DefaultMappedFile.java`、`AbstractMappedFile.java`、`MappedFileQueue.java`
> 阅读建议：这篇不要只理解成“用了 mmap”，重点是三套写入模式、三个位置指针和文件队列管理。

## 先给结论

- `MappedFile` 是 RocketMQ 存储层的统一文件抽象，不只服务 CommitLog，也服务 ConsumeQueue、Index、TimerLog 等文件型存储。
- `DefaultMappedFile` 默认用 `MappedByteBuffer` 做读写，但 5.5.0 也显式支持 `writeWithoutMmap` 和 `TransientStorePool` 两种变体。
- `MappedFileQueue` 管的是一串定长、按 offset 命名的逻辑连续文件，它解决的核心问题不是“存文件”，而是“按物理 offset O(1) 定位文件分片”。
- 写入链最关键的不是一个 `wrotePosition`，而是 `wrotePosition / committedPosition / flushedPosition` 三个边界。
- RocketMQ 的“顺序写性能”并不只来自 CommitLog 顺序追加，也来自这套定长文件 + PageCache + 预分配的统一底座。

## 为什么 store 层要先抽象 `MappedFile`

如果只从 Java NIO 角度理解，很容易把它简化成：

- 打开文件
- map 到内存
- append
- flush

但 RocketMQ 实际上还要统一处理这些问题：

- 文件起始 offset 和命名
- 多文件滚动
- 写入位置推进
- commit / flush 分离
- 预创建下一个文件
- 资源释放和清理

这些问题如果直接散在 `CommitLog`、`ConsumeQueue`、`IndexFile` 里，存储层会非常难维护。

所以 `MappedFile` 的意义是：

- 把文件级别的通用写入行为抽成底座

## `DefaultMappedFile` 的核心状态

源码里最值得记住的是这些字段：

- `fileName`
- `fileFromOffset`
- `fileSize`
- `fileChannel`
- `mappedByteBuffer`
- `writeBuffer`
- `wrotePosition`
- `committedPosition`
- `flushedPosition`

其中最关键的不是 mmap 本身，而是三个位置指针。

### 1. [[26-存储架构总览]]`wrotePosition`

表示消息已经写进当前写入缓冲区的最大位置。

### 2. [[28-CommitLog写入流程]]`committedPosition`

只在启用 `TransientStorePool` 时真正体现意义，表示：

- 堆外临时缓冲区的数据已经提交到了 `FileChannel` / PageCache 的位置

### 3. [[31-刷盘机制]] [[31-刷盘机制]]`flushedPosition`

表示已经真正执行 flush 的位置。

所以这三个位置不是重复计数，而是三道边界：

- 已写入
- 已提交到 PageCache
- 已刷盘

## RocketMQ 在 5.5.0 里实际存在三种写入模式

### 1. [[26-存储架构总览]]默认模式：`MappedByteBuffer`

最常见。

特点：

- 读写都基于 mmap
- 数据先进入 OS PageCache
- flush 时调用 `mappedByteBuffer.force()`

这也是大多数“RocketMQ 用 mmap 顺序写”的来源。

### 2. [[28-CommitLog写入流程]]`writeWithoutMmap`

5.5.0 的 `DefaultMappedFile` 明确支持：

- 写入不用 `MappedByteBuffer`
- 仍保留只读 mmap 供读取路径使用

初始化逻辑很直接：

```java
if (writeWithoutMmap) {
    this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_ONLY, 0, fileSize);
} else {
    this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
}
```

这说明：

- “RocketMQ 一定用 mmap 写”在 5.5.0 已经不是绝对命题

### 3. [[31-刷盘机制]] [[31-刷盘机制]]`TransientStorePool`

这是更高吞吐但更复杂的模式。

它会先借一个堆外缓冲：

```java
this.writeBuffer = transientStorePool.borrowBuffer();
```

然后形成两阶段路径：

1. [[26-存储架构总览]]先写入 `writeBuffer`
2. [[28-CommitLog写入流程]]再 commit 到 `FileChannel` / PageCache
3. [[31-刷盘机制]] [[31-刷盘机制]]最后 flush 到磁盘

所以这套模式下：

- `wrote != committed != flushed`

## `MappedFileQueue` 解决的是“文件链”问题

单个 `MappedFile` 只代表一个定长文件片段。

真正让 CommitLog / ConsumeQueue 形成无限追加逻辑空间的是 `MappedFileQueue`。

它的核心状态包括：

- `storePath`
- `mappedFileSize`
- `mappedFiles`
- `flushedWhere`
- `committedWhere`

注意这里维护的是全局位置，而不是单文件位置。

### 为什么可以 O(1) 找到文件

前提有两个：

1. [[26-存储架构总览]]所有 mapped file 定长
2. [[28-CommitLog写入流程]]文件名就是起始 offset

于是可以直接按除法定位：

- `offset / mappedFileSize`

再减去首文件基准，就能跳到目标下标，而不需要线性遍历整个文件列表。

这也是 `MappedFileQueue` 成立的关键前提。

## 预创建文件不是小优化，而是主链的一部分

`MappedFileQueue` 可以依赖 `AllocateMappedFileService` 提前准备后续文件。

这解决的是：

- 当前文件写满时，创建新文件的成本不能阻塞主写入路径

所以 RocketMQ 的“顺序写”不是只靠 PageCache，而是还靠：

- 文件滚动切换成本被前移

## 存储层为什么敢用 `CopyOnWriteArrayList`

`MappedFileQueue.mappedFiles` 使用的是 `CopyOnWriteArrayList`。

这里不是泛用并发容器选择，而是基于实际访问模式：

- 读多写少
- 文件滚动和删除远少于读取与定位

所以它优化的是：

- 读路径稳定遍历
- 写路径接受低频复制成本

## 恢复和裁剪也依赖这层底座

`MappedFileQueue.truncateDirtyFiles(offset)` 会：

- 找到超出目标 offset 的文件
- 如果 offset 落在文件内部，就把三个位置指针截到该位置
- 如果整个文件都在 offset 之后，就直接销毁并移除

这说明 `MappedFileQueue` 不是单纯运行时队列，也承担：

- 崩溃恢复后的文件边界修正
- 逻辑脏尾截断

## 读时间索引为什么也落在 `MappedFileQueue`

在 ConsumeQueue 按时间查 offset 时，`MappedFileQueue.getConsumeQueueMappedFileByTime(...)` 会先给每个逻辑文件补：

- `startTimestamp`
- `stopTimestamp`

它不是依赖文件系统 `lastModifiedTime`，而是回查 CommitLog 的消息存储时间。

这说明 `MappedFileQueue` 在 5.5.0 里还承担了：

- 文件级时间边界缓存

## 这篇真正要记住的边界

### `MappedFile` 不是 CommitLog 专属

它是整个文件型存储的底座。

### mmap 不是唯一写路径

5.5.0 明确支持：

- 默认 mmap 写
- `writeWithoutMmap`
- `TransientStorePool`

### 文件队列能 O(1) 定位的前提是“定长 + offset 命名”

如果这两个前提破坏，整个定位模型就不成立。

### 三个位置指针决定了刷盘和可见性边界

后续看 CommitLog 刷盘、HA 和同步确认时，这三个指针必须一直带着。

## 和后续笔记的关系

-  会在这套底座上继续看消息如何追加。
-  会专门解释 `commit` / `flush` 的运行时线程。
-  会解释为什么 ConsumeQueue 也沿用这套文件队列模型。

## 建议连读

1. [[26-存储架构总览]]
2. [[28-CommitLog写入流程]]
3. [[31-刷盘机制]]