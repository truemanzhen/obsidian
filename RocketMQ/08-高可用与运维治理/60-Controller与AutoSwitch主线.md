# Controller 与 AutoSwitch 主线

> 研究定位：把 5.5.0 的 Controller 模式、AutoSwitchHA、Broker 本地元数据文件和角色切换主线拉成一条完整链，解决“Controller 到底管什么、Broker 怎么真正切主”的问题。
> 关键源码：`controller/.../ControllerManager.java`、`controller/.../ReplicasInfoManager.java`、`broker/.../controller/ReplicasManager.java`、`store/.../ha/autoswitch/AutoSwitchHAService.java`、`store/.../ha/autoswitch/EpochFileCache.java`
> 阅读建议：不要把它只读成“另一个选主模块”。这条线同时包含 controller 元数据、brokerId 分配、syncStateSet、epoch 与 Broker 角色切换。

## 先给结论

- Controller 模式的核心不是“替 Broker 同步数据”，而是“集中维护副本集元数据并做主从决策”。
- AutoSwitchHA 负责的是 Broker 存储侧真正切主/切从、epoch 文件、确认点和截断逻辑。
- 两者之间的桥是 `ReplicasManager`：它既和 Controller 通信，也驱动本地 `AutoSwitchHAService`。
- 5.5.0 这条链路里，Broker 启动时并不是直接拿一个固定 brokerId，而是可能先向 Controller 申请 `brokerId` 并落本地 metadata 文件。
- 如果只看 `DLedger` 或只看 `DefaultHA`，都无法解释 5.5.0 Controller 模式的完整实现。

## 先分三层职责

### 1. [[59-HA主从同步设计]]Controller：维护副本集元数据与选主决策

核心对象：

- `ControllerManager`
- `ControllerRequestProcessor`
- `ReplicasInfoManager`

它回答的问题是：

- 这个 broker-set 有哪些副本
- 当前 master 是谁
- `syncStateSet` 是什么
- 是否需要 elect master

### 2. [[61-HA服务实现细节]]Broker 控制层：把远端决策变成本地角色切换

核心对象：

- `ReplicasManager`
- `BrokerOuterAPI`

它回答的问题是：

- 本节点现在该当 master 还是 slave
- 要不要向 Controller 申请 brokerId
- 要不要注册自己
- 收到角色变化后本地怎么切

### 3. [[62-DLedger与Raft机制]]存储 HA 层：真的完成切主/切从

核心对象：

- `AutoSwitchHAService`
- `EpochFileCache`
- `BrokerMetadata`
- `TempBrokerMetadata`

它回答的问题是：

- 切主前后怎么处理 epoch
- 如何截断脏数据
- 如何计算 confirm offset
- slave 如何连新 master 继续追

## Broker 首次接入 Controller 的注册主线

`ReplicasManager` 里把这条链写得很明确：

1. [[59-HA主从同步设计]]`getNextBrokerId()`
2. [[61-HA服务实现细节]]`createTempMetadataFile(nextBrokerId)`
3. [[62-DLedger与Raft机制]]`applyBrokerId()`
4. `createMetadataFileAndDeleteTemp()`
5. `registerBrokerToController()`

对应状态机会推进：

- `INITIAL`
- `CREATE_TEMP_METADATA_FILE_DONE`
- `CREATE_METADATA_FILE_DONE`
- `REGISTERED`

这里最容易漏掉的点是：

- brokerId 不是永远从配置里硬编码来的
- Controller 模式下，Broker 可能先拿一个临时 metadata，再申请正式 brokerId

## 本地为什么要有 `TempBrokerMetadata` 和 `BrokerMetadata`

`ReplicasManager` 用两份文件解决“申请 brokerId 过程中的一致性问题”：

- `TempBrokerMetadata`：记录“我准备申请哪个 brokerId + registerCheckCode”
- `BrokerMetadata`：记录“这个 brokerId 已经正式属于我”

这能避免：

- 申请过程中进程退出后状态丢失
- 同一个 broker-set 里多个节点冲突复用 brokerId

所以这里的本地 metadata 文件不是附属细节，而是整个注册流程的一部分。

## Controller 侧到底维护了什么

`ReplicasInfoManager` 的几条关键方法就足够勾出主线：

- `getNextBrokerId(...)`
- `applyBrokerId(...)`
- `registerBroker(...)`
- `electMaster(...)`
- `alterSyncStateSet(...)`
- `getReplicaInfo(...)`

其中最关键的是 `electMaster(...)`：

- 先看这个 broker-set 是否存在
- 再看老 master 是否还有效
- 再按 `ElectPolicy` 从 `syncStateSet` 或允许范围里选出新 master
- 成功后把 `masterEpoch`、`syncStateSetEpoch` 一起推进

这说明 Controller 不是“广播谁是主”，而是在维护一套版本化副本集状态。

## `syncStateSet` 是这条线的另一个中心对象

你可以把它理解成：

- 当前认为已经同步到安全范围内、可参与高可靠确认的副本集合

它的变化路径包括：

- Controller 端 `alterSyncStateSet(...)`
- Broker 端 `ReplicasManager.changeSyncStateSet(...)`
- HA 层 `AutoSwitchHAService.setSyncStateSet(...)`

这意味着 `syncStateSet` 不是单纯配置，而是：

- 确认点计算
- 主从切换
- 副本健康判断

的共同输入。

## 角色变化真正落地在 `ReplicasManager`

`ReplicasManager.changeBrokerRole(...)` 会根据：

- `newMasterBrokerId`
- `newMasterAddress`
- `newMasterEpoch`
- `syncStateSetEpoch`
- `syncStateSet`

分流到：

- `changeToMaster(...)`
- `changeToSlave(...)`

### 切成 master 时它会做什么

核心动作包括：

- 更新 `masterEpoch`
- 更新 `syncStateSet`
- 调 `haService.changeToMaster(...)`
- 把 `brokerId` 设成 `MixAll.MASTER_ID`
- 把 `brokerRole` 设成 `SYNC_MASTER`
- 开启特殊服务
- 重新向 NameServer 注册

### 切成 slave 时它会做什么

核心动作包括：

- 停止 `syncStateSet` 检查任务
- 把 `brokerRole` 改成 `SLAVE`
- 设置 `masterAddress`
- 调 `haService.changeToSlave(...)`
- 配置 `SlaveSynchronize`
- 重新向 NameServer 注册

所以真正的角色切换不是 Controller 自己直接改 Broker，而是 Broker 收到结果后本地执行切换流程。

## AutoSwitchHAService 解决的是“存储如何无歧义切换”

切主时它最关键的几步是：

1. [[59-HA主从同步设计]]销毁旧连接
2. [[61-HA服务实现细节]]关闭旧 haClient
3. [[62-DLedger与Raft机制]]`truncateInvalidMsg()`
4. `setConfirmOffset(computeConfirmOffset())`
5. 依据 `masterEpoch` 更新 `EpochFileCache`
6. 等待 dispatch 落后字节清空
7. 恢复 topic-queue table

这说明切主不是单纯改个角色位，而是要先把存储状态修到一致点。

切从时则会：

1. [[59-HA主从同步设计]]建立/重开 `AutoSwitchHAClient`
2. [[61-HA服务实现细节]]指向新 master 地址
3. [[62-DLedger与Raft机制]]启动 slave 追赶
4. 更新本地 state machine version

## 为什么 epoch 文件这么重要

`EpochFileCache` 记录的是：

- 每个 epoch 的起始 offset

它的用途不是做历史展示，而是：

- 找一致点
- 截断脏数据
- 保证新主/新从知道哪些数据属于哪个任期

这也是 5.5.0 里“脑裂防护”真正落地的本地状态之一。

## 这条主线和 DLedger 的关系

它们有关，但不是同一件事。

-  更偏“共识/选主机制”
- 这篇更偏“Controller + Broker + HAService 的完整落地路径”

具体来说：

- Controller 可能由 DLedgerController 或 JRaftController 承载
- 但 Broker 侧始终要经过 `ReplicasManager` 和 `AutoSwitchHAService`

所以如果只读 DLedger，不会看到 brokerId 文件、syncStateSet 上报、NameServer 重新注册这些落地环节。

## 研究时最值得继续看什么

- `registerBrokerToController()` 返回 master 信息后，Broker 为什么可能直接在注册阶段切成 master/slave。
- `doReportSyncStateSetChanged` 和定时 `checkSyncStateSetAndDoReport` 何时触发。
- `computeConfirmOffset()` 如何与 `syncStateSet` 联动。
- `changeToMasterWhenLastRoleIsMaster(...)` 为什么能省掉部分切换动作。

## 建议连读