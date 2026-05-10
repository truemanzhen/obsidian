# MappedFile 与内存映射 — 文件操作底层

> 📄 源码路径：`store/src/main/java/org/apache/rocketmq/store/logfile/`
> 🏷️ #核心类 #面试高频

## 🎯 核心类

| 类 | 职责 |
|----|------|
| `MappedFile` | 接口，定义文件操作 API |
| `DefaultMappedFile` | 实现，基于 MappedByteBuffer / FileChannel |
| `MappedFileQueue` | 管理一组有序的 MappedFile |

## 📐 MappedFile — 单个映射文件

### 核心字段

```java
public class DefaultMappedFile implements MappedFile {
    protected final String fileName;           // 文件名 = 起始 offset
    protected final int fileSize;              // 文件大小（如 1GB）
    protected FileChannel fileChannel;         // 文件通道
    protected MappedByteBuffer mappedByteBuffer; // 内存映射缓冲区
    protected int wrotePosition = 0;           // 已写位置
    protected int flushedPosition = 0;         // 已刷盘位置
    protected int committedPosition = 0;       // 已提交位置（TransientStorePool）
}
```

### 三种 I/O 模式

```
模式 1：MappedByteBuffer（默认）
┌─────────────────────────────────────┐
│           JVM 堆外内存               │
│  ┌─────────────────────────────┐    │
│  │   MappedByteBuffer          │    │
│  │   (直接映射到磁盘文件)       │    │
│  └──────────────┬──────────────┘    │
│                 │                   │
│          OS PageCache               │
│                 │                   │
│           磁盘文件                   │
└─────────────────────────────────────┘
写入：直接写 PageCache → OS 异步刷盘

模式 2：FileChannel（writeWithoutMmap=true）
┌─────────────────────────────────────┐
│           JVM 堆内存                 │
│  ┌─────────────────────────────┐    │
│  │   HeapByteBuffer            │    │
│  └──────────────┬──────────────┘    │
│                 │ write()           │
│          FileChannel                │
│                 │                   │
│          OS PageCache               │
│                 │                   │
│           磁盘文件                   │
└─────────────────────────────────────┘

模式 3：TransientStorePool（堆外内存池）
┌─────────────────────────────────────┐
│      堆外内存池（预分配）             │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐      │
│  │Buf0│ │Buf1│ │Buf2│ │Buf3│      │
│  └──┬─┘ └──┬─┘ └──┬─┘ └──┬─┘      │
│     └──────┴──────┴──────┘         │
│              │ commit()             │
│         FileChannel                 │
│              │                      │
│         OS PageCache                │
│              │                      │
│          磁盘文件                    │
└─────────────────────────────────────┘
```

> [!important] 三种模式对比
> | 模式 | 写入目标 | 刷盘方式 | 适用场景 |
> |------|----------|----------|----------|
> | MappedByteBuffer | PageCache 直写 | OS 管理 | 默认，简单高效 |
> | FileChannel | 堆内存 → FileChannel.write() | 需要显式 flush | 兼容性好 |
> | TransientStorePool | 堆外池 → commit → flush | 两次提交 | 高吞吐场景 |

## 📐 MappedFileQueue — 文件队列

```java
public class MappedFileQueue {
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles;  // 有序文件列表
    protected final int mappedFileSize;                            // 单文件大小
    protected long flushedWhere = 0;                               // 全局刷盘位置
    protected long committedWhere = 0;                             // 全局提交位置
}
```

### 核心操作

#### 1. 按 offset 查找文件 — O(1)

```java
public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
    MappedFile firstMappedFile = this.getFirstMappedFile();
    MappedFile lastMappedFile = this.getLastMappedFile();

    // 边界检查
    if (offset < firstMappedFile.getFileFromOffset()
        || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
        // offset 超出范围
    } else {
        // O(1) 直接计算索引！
        int index = (int) ((offset / this.mappedFileSize)
            - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
        MappedFile targetFile = this.mappedFiles.get(index);

        // 验证
        if (offset >= targetFile.getFileFromOffset()
            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
            return targetFile;
        }
    }
}
```

> [!tip] O(1) 查找的秘密
> 文件名就是起始 offset（如 `00000000001073741824`），
> 所有文件大小相同（如 1GB），所以：
> ```
> index = offset / fileSize - firstFileOffset / fileSize
> ```
> 直接用除法算出是第几个文件，不需要遍历。

#### 2. 获取/创建最新文件

```java
public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
    MappedFile mappedFileLast = getLastMappedFile();

    if (mappedFileLast == null) {
        // 队列为空，从 startOffset 对齐位置创建
        createOffset = startOffset - (startOffset % this.mappedFileSize);
    } else if (mappedFileLast.isFull()) {
        // 最后一个文件已满，创建下一个
        createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
    }

    if (createOffset != -1 && needCreate) {
        return tryCreateMappedFile(createOffset);
    }
    return mappedFileLast;
}
```

#### 3. 文件预分配（双缓冲）

```java
protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
    MappedFile mappedFile = null;

    if (this.allocateMappedFileService != null) {
        // 通过异步服务预分配（双缓冲）
        mappedFile = this.allocateMappedFileService
            .putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
    } else {
        // 直接创建
        mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
    }
}
```

> [!tip] 双缓冲预分配
> `AllocateMappedFileService` 会**提前创建下一个文件**：
> - 当前文件：`00000000000000000000`
> - 预创建：`00000000001073741824`（下一个 1GB 位置）
>
> 这样当前文件写满时，可以**零等待**切换到新文件。

## 📐 文件生命周期：三个 Position

```
MappedFile 内部有三个指针：

  ┌──────────────────────────────────────────────┐
  │ MappedFile (1GB)                             │
  │                                              │
  │  0        committed    flushed    wrote      │
  │  │            │            │         │       │
  │  ▼            ▼            ▼         ▼       │
  │  [====已提交====|===已刷盘===|==未刷盘==|  空  │
  │                                              │
  └──────────────────────────────────────────────┘

  wrotePosition:    最新写入位置（生产者最新写到哪）
  committedPosition: 已提交到 PageCache（TransientStorePool 模式）
  flushedPosition:  已刷到磁盘（持久化完成）
```

| 模式 | wrote → committed | committed → flushed |
|------|-------------------|---------------------|
| MappedByteBuffer | 同一个位置 | OS 异步刷盘 |
| TransientStorePool | commit() 提交 | flush() 刷盘 |

## 💡 设计亮点总结

| 设计 | 解决什么问题 |
|------|-------------|
| **内存映射** | 避免用户态/内核态拷贝，直接写 PageCache |
| **O(1) 文件查找** | 文件名=offset，除法计算索引 |
| **双缓冲预分配** | 文件写满时零等待切换 |
| **CopyOnWriteArrayList** | 读多写少场景，遍历时不阻塞 |
| **三种 I/O 模式** | 灵活适配不同硬件和场景 |

---

> ⏭️ 下一篇：[[11-CommitLog写入流程]] — 消息写入的完整链路
