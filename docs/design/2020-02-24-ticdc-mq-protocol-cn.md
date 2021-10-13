# TiCDC MQ(Kafka) Protocol

## 设计目标

- 尽可能复用现有的 Sink 接口
- 满足事务级别的一致性
- 满足下游可以恢复到某个一致的 ts
- 提供一个开关，可以弱化一致性约束提高同步效率，满足行级别的一致性
- 尽可能使下游易于开发
- 利用 Kafka partition 提供平行扩展能力

## 术语表

### Kafka 相关概念

| 名词      | 定义 | 解释                                                                                                                  |
| --------- | ---- | --------------------------------------------------------------------------------------------------------------------- |
| Partition | -    | Kafka 中的概念，一个消息队列（Topic）中包含数个 Partition。Partition 内部保证消息时序，Partition 之间不保证消息时序。 |
| Producer  | -    | 消息生产者，向 Kafka 集群写入 Msg，在本协议中，Producer 指 TiCDC 集群。                                               |
| Consumer  | -    | 消息消费者，从 Kafka 队列进行消费的程序。                                                                             |

### TiCDC 相关概念

| 名词                       | 定义                                                                             | 解释                                                                                                                 |
| -------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| CDC Owner                  | -                                                                                | CDC 集群节点的角色之一，一个 CDC 集群有且只有一个 Owner 节点。Owner 节点负责协调各个 Processor 的进度，同步 DDL 等。 |
| CDC Processor              | -                                                                                | CDC 集群节点的角色之一，一个 CDC 集群有数个 Processor 节点。Processor 节点执行对一张或多张表的的数据变更进行同步。   |
| CDC Processor ResolvedTS   | ResolvedTS < ∀(unapplied CommitTS)                                               | ResolvedTS 保证单个对应 Processor 的任意 CommitTS 小于 ResolvedTS 的 Change Event 已经被输出到 CDC 集群。            |
| CDC Processor CheckpointTS | CheckpointTS < ∀(unduplicated && unapplied CommitTS); CheckpointTS <= ResolvedTS | CheckpointTS 保证单个对应 Processor 的任意 CommitTS 小于 CheckpointTs 的 Change Event 已经被同步到对应 Sink。        |
| CDC DDL Puller ResolvedTS  | ResolvedTS < ∀(FinishedTS of undone DDL Job)                                     | CDC DDLPuller ResolvedTS 保证任意 FinishedTS 小于 CDC DDLPuller ResolvedTS 的 DDL 已经被输出到 CDC 集群。            |
| CDC Global ResolvedTS      | Global ResolvedTS = min(all Processor ResolvedTS)                                | CDC Global ResolvedTS 是各个 Processor 当前 ResolvedTS 的最小值                                                      |
| CDC Global CheckpointTS    | Global CheckpointTS = min(all Processor CheckpointTS)                            | CDC Global CheckpointTS 是各个 Processor 当前 CheckpointTS 的最小值                                                  |
| DDL FinishedTS             | DDL 执行完成（DDL Job Status 转为 Done） 的事务的 CommitTS                       | 标记 DDL 何时被应用，用于 DDL 与 DML 之间的排序。                                                                    |

## 消息类型

- Row Changed Event：由 CDC Processor 产生，包含 Txn CommitTS、Row Value 相关数据

```
示例：
Key:
{
    "ts":1,
    "type":"Row",
    "schema":"schema1",
    "table":"table1"
}
Value:
{
    "update":{
        "columnName1":{
            "type":"Long",
            "value":7766,
            "unique":true
        },
        "columnName2":{
            "type":"Text",
            "value":"aabb"
        }
    }
}

{
    "delete":{
        "columnName1":{
            "type":"Long",
            "value":7766,
            "unique":true
        }
    }
}
```

- DDL Event：由 CDC Owner 产生，包含 DDL FinishedTS、DDL query

```
示例：
Key：
{
    "ts":1,
    "type":"DDL",
    "schema":"schema1",
    "table":"table1"
}
Value：
{
    "query":"drop table a;"
}
```

- Resolved Event：由 CDC Owner 产生，包含 ResolvedTS

```
示例：
Key:
{
    "ts":1,
    "type":"Resolved"
}
Value:
-
```

- Admin Event：由 CDC Owner 产生，目前用于 partition 扩容的情况。

```
示例：
Key:
{
    "type":"Admin"
}
Value:
{
    "partition":5
}
```

## 约束

1. 对于一行数据的变更：
   1. 一定会分发在同一个 partition 中
   1. 大多数情况下，每一个变更只会发出一次，在 CDC Processor 意外下线时，可能会发送重复的变更
   1. 每个变更第一次出现的次序一定是有序的（CommitTS 升序）
1. 对于一个 partition：
   1. 不保证不同 row 的 Row Changed Event 之间的时序
   1. Resolved Event 保证任意（CommitTS < ResolvedTS）的 Row Changed Event 在 Resolved Event 之后不会出现。
   1. DDL Event 保证任意（CommitTS < FinishedTS of DDL event）的 Row Changed Event 在 DDL Event 之后不会出现。
   1. Resolved Event 和 DDL Event 会广播到所有的 partition

## Kafka 分区策略

- Row Changed Event 根据一定的分区算法分发至不同的 partition
- Resolved Event 和 DDL Event 广播至所有的 partition

### Row Changed Event 划分 Partition 算法

#### \_tidb_row_id 分发

使用 \_tidb_row_id 计算 hash 分发到 partition

使用限制：

- 下游非 RDMS，或下游接受维护 \_tidb_row_id
- 上游表 pk is handle

#### 基于 uk、pk 分发

使用 pk(not handle) or uk(not null) 的 value 计算 hash 分发到 partition

使用限制：

- 表内只能包含一个 pk or uk

#### 基于 table 分发

使用 schemaID 和 tableID 计算 hash 分发到 partition

注：在当前的实现中，基于 table 分发使用的是 schemaName 与 tableName 的值计算 hash

使用限制：

- 无，但是这种分发方式粒度粗，partition 之间负载不均

#### 基于 ts 分发

使用 commit_ts 取模分发到 partition

使用限制：

- 无法保证行内有序性和表内有序性

#### 冲突检测分发

检测 pk or uk 冲突，将不冲突的 row 写入不同的 partition。发生冲突时向下游写入用于对齐进度的 Event

注：尚未实现这种分发方式

使用限制：

- 只有表中 pk or uk 数量大于 1 才适用这种方式
- 下游消费端实现复杂

#### 分发方式与一致性保证

|      分发方式      | 行内有序性<sup><a href="#note1">[1]</a></sup> | 表内有序性<sup><a href="#note2">[2]</a></sup> | 表内事务一致性 <sup><a href="#note3">[3]</a></sup> | partition 分配平衡度 |
| :----------------: | :-------------------------------------------: | :-------------------------------------------: | :------------------------------------------------: | :------------------: |
| \_tidb_row_id 分发 |     ➖<sup><a href="#note4">[4]</a></sup>     |                       ✖                       |                         ✖                          |         平衡         |
|  基于 uk、pk 分发  |     ➖<sup><a href="#note5">[5]</a></sup>     |                       ✖                       |                         ✖                          |         平衡         |
|  基于 table 分发   |                       ✔                       |                       ✔                       |                         ✔                          |        不平衡        |
|    冲突检测分发    |     ➖<sup><a href="#note6">[6]</a></sup>     |                       ✖                       |       ➖<sup><a href="#note6">[6]</a></sup>        |        较平衡        |
|    基于 ts 分发    |                       ✖                       |                       ✖                       |                         ✖                          |         平衡         |

1. <a name="note1"></a> 行内有序性指对于某一行产生的一组数据变更事件，一定会被分发到同一个 partition，并且在 partition 内保证时间增序
1. <a name="note2"></a> 表内有序性指对于某一表内产生的一组数据变更事件，一定会被分发到同一个 partition，并且在 partition 内保证时间增序
1. <a name="note3"></a> 表内事务一致性指对于某一事务产生的每一表内的一组数据变更事件，一定会被分发到同一个 partition，并且在 partition 内保证时间增序
1. <a name="note4"></a> 仅当上游表内只有一个主键且主键为 int 类型时，\_tidb_row_id 分发满足行内有序性
1. <a name="note5"></a> 仅当上游表内只有一个主键或唯一索引时，基于 uk、pk 分发满足行内有序性
1. <a name="note6"></a> 对于冲突检测分发，当未检测到冲突时，消费端可以并发读取 partition 中的数据变更事件，当检测到冲突时，下游需要对齐各个 partition 的消费进度，当未检测到冲突时，冲突检测分发是满足行内有序性和表内事务一致性的。

### Partition 扩容与缩容

#### 基础方案

CDC 集群：

- CDC Owner
  - 暂停所有任务
    - 令 CDC Global ResolvedTS = 0（或发送 AdminStop 指令）
    - 等待所有 CDC Processor 的任务已经暂停（此处缺少 Processor 反馈机制）
  - 通知所有 CDC Processor，Partition 数量变更
  - 向 kafka 所有 Partition 广播 Admin Event，包含新的 Partition 数量信息
  - 恢复所有任务
    - 令 StartTS = CDC Global CheckpointTS 重启任务
- CDC Consumer
  - 收到 Admin Event 后，丢弃缓存中（CommitTS > Kafka Partition ResolvedTS) Row Changed Event，等待其他 listener 都收到 Admin Event。
  - 全部 listener 收到 Admin Event 后，按照新的 partition 数量重新分配 listener

注：第一版暂不实现 partition 的扩容与缩容。

## Kafka Producer 逻辑

### CDC 同步模型

1. 基础方案

直接在目前的 CDC 同步模型的基础上实现，目前 CDC 同步模型中，对于一个 CDC Processor 而言，CDC Processor 以 Table 为单位，捕获 TiKV Change Event。通过 Resolved TS 机制排序，将相同 CommitTS 的 TiKV Row Change Event 拼成一个事务。保证了表内事务的完整性和有序性。

TiKV Row Change Event 不需要经过拼事务的过程（CollectRawTxns Function），但是需要利用 ResolvedTS 排序，保证 Table 内 Row 级别的有序输出到 Kafka。

CDC 同步模型中的其他机制保持不变。

因此，基础方案符合上文“1”约束。

CDC 同步模型中，CDC Global CheckpointTS 的意义是所有（CommitTS <= CDC Global CheckpointT）的事务已经被写入到下游，如果下游是 Kafka，CDC Global CheckpointTS 的意义则是所有（CommitTS <= CDC CheckpointTS）的 Change Event 已经被写入到 Kafka。

因此，在 CDC 同步模型中的 CDC Global CheckpointTS 符合上文“2.2”约束。

对于 DDL 而言，CDC 同步模型中，当 CDC Global CheckpointTS = DDL FinishedTS 时才会向下游复制 DDL 信息，DDL FinishedTS 同样具有 CDC Global CheckpointTS 的意义。

因此，CDC DDL FinishedTS 符合上文“2.3”约束。

2. 在基础方案上的优化

以 row 为单位向 kafka 写数据，CDC 集群不需要还原事务。Owner 只需要维护 DDL 和 DML 的时序，可以令 `CDC Global ResolvedTS = MaxUint64`，而不是 `CDC Global ResolvedTS = min(all Processor ResolvedTS)`。`CDC Global CheckpointTS = min(CDC DDL Puller ResolvedTS, DDL Finished TS, min(all Processor CheckpointTS))`，而不是 `CDC Global CheckpointTS = min(all Processor CheckpointTS)`。

正确性论证：

我们称：

- 尚未复制到 Kafka 的 Change Event 为 unwritten Change Event；
- 在 TiKV 中尚未输出到 CDC 集群并且不与已经输出到 CDC 集群重复的的 Change Event 为 unapplied Change Event；
- 状态尚未变成 Done 的 DDL 为 undone DDL。

“2.2” 约束的公式描述：

`Kafka Partition ResolvedTS < ∀(unwritten CommitTS in all CDC Processors)`

结合 CDC Processor ResolvedTS 定义，有：

`min(all Processor CheckpointTS) < ∀(unwritten CommitTS in all CDC Processors)`

结合优化中新的 CDC Global CheckpointTS 定义有：

`CDC Global CheckpointTS <= min(all Processor CheckpointTS) < ∀(unwritten CommitTS in all CDC Processors)`

即 Global CheckpointTS 满足“2.2”约束。

根据 CDC 同步模型，当且仅当 `CDC Global ResolvedTS = DDL FinishedTS 时，CDC Owner` 向下游写 DDL Event。
因此，写 DDL Event 时，`DDL FinishedTS < ∀(unwritten CommitTS in all CDC Processors)`

即 DDL FinishedTS 满足“2.3”约束。

### CDC Owner 逻辑

- 向所有 partition 广播有序的 CDC Global CheckpointTS（作为 Kafka Partition ResolvedTS）。
- 当 Kafka Partition ResolvedTS = DDL FinishedTS 时，向所有 partition 广播 DDL Event。

### CDC Processor 逻辑

- 对于一个 Row Event：
  - Msg Key = (SchemaName，TableName，CommitTS) , Msg Value = Change Event Value。
  - 根据相应的分区算法计算 Hash，确定 partition，写入到对应的 partition。
- 对于一个 Processor
  - Processor 需要保证按 CommitTS 递增顺序输出 Row Changed Event。

## Kafka Consumer 逻辑

需要满足前提：Kafka 集群可以正常读取。

1. 每一个 partition 对应一个 listener
2. 对于每一个 listener，首先缓存接收到的 Row Changed Event
3. 收到 ResolvedTS 后，执行被缓存的 (Commit TS <= ResolvedTS) 的 Row Changed Event (commit TS <= ResolvedTS)
   1. 不要求还原事务：单个 listener 收到 resolved ts 之后就可以写下游
   1. 要求还原事务：需要所有的 listener 都收到 resolvedts，还原事务后写下游
4. 收到 DDL event 后，执行被缓存的 (Commit TS <= DDL FinishedTS) 的 Row Changed Event，并且等待其他 listener 都读到 DDL event
5. 当所有的 listener 都收到 DDL 后，执行 DDL，并向下推进。

## 几种特殊情况的处理

### CDC Owner 切换

没有特殊情况需要处理

#### CDC Owner 切换时 global checkpoint TS 会回退吗？

不会，CDC 同步模型不允许 global checkpoint TS 回退。

### Table 迁移

Table 迁移在 CDC 集群中体现在，某 Table 由 ProcessorA 迁移到 ProcessorB。
由于 Row Changed Event 按照 RowID Hash 分片，因此 Table 迁移后，新的 Processor 仍然会保持原有的分片结果，对于消费端而言，Table 的迁移是无感知的。不需要额外处理。

> Table 迁移可能导致 Row Changed Event 的重复写入，见【Processor 异常下线】一节。

### CDC Processor 下线

#### CDC Processor 正常下线

消费端无感知，不需要额外处理。

#### CDC Processor 异常下线

CDC Processor 异常下线后，由 CDC Owner 感知并标记异常 Processor 下线。有可能出现 CDC Processor 重复写入 Row Changed Event 的情况：

假设有 ProcessorA、ProcessorB，ProcessorA 同步 TableA

1. ProcessorA 向 kafka 写入四个 Row Changed Event (TS=1,2,3,4)
2. ProcessorA 意外下线，ProcessorA 的 CheckpointTS 记录到 2
3. Owner 标记 ProcessorA 下线，并且将 TableA 迁移到 ProcessorB
4. ProcessorB 向 kafka 写入 Txn，StartTS=2，写入了 Row Changed Event（TS=2，3，4）

因此同一 partition 中可能出现重复的 Row Changed Event。
重复的 Row Changed Event **不会**被分发到不同 partition。

解决方案：消费端需要记录每一张表的已经执行的最大 TS（CheckpointTS），过滤掉小于 CheckpointTS 的 Txn

## 未来计划

### 消费端实现

由于 Kafka 消费端逻辑和现有的 CDC 集群写下游逻辑类似，实现上考虑复用 CDC 写下游的逻辑，kafka 相当于 tikv 外的另一种上游数据源。

目前考虑到 tikv cdc 是以 table 为单位分片，kafka 以 row id 为单位 hash 分片，下游实现起来会有区别，需要考虑兼容方案。
