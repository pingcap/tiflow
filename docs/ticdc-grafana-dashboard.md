---
title: TiCDC 重要监控指标详解
---

<!--

How to obatin pictures in this document?

1. Deploy two cluster

 upstream: 1 TiDB, 1 PD, 4 TiKV and 2 TiCDC
 upstream: 1 TiDB, 1 PD and 1 TiKV

2. Create changefeed

    ```sh
    tiup cdc cli --pd=http://172.16.5.33:47902 changefeed create \
        --sink-uri="mysql://root:@172.16.5.33:48004/" \
        --changefeed-id="simple-replication-task"
    ```

3. Run TPCC

    ```sh
    go-tpc -H 172.16.5.33 -P 47904 -U root -D test tpcc prepare --warehouses 30 && \
        go-tpc -H 172.16.5.33 -P 47904 -U root -D test tpcc run --warehouses 30
    ```

-->


# TiCDC 重要监控指标详解

使用 TiUP 部署 TiDB 集群时，一键部署监控系统 (Prometheus & Grafana)，监控架构参见 [TiDB 监控框架概述](https://docs.pingcap.com/zh/tidb/stable/tidb-monitoring-framework)。

对于日常运维，我们通过观察 TiCDC 面板上的 Metrics，可以了解 TiCDC 当前的状态。

本文档使用默认配置创建一个同步到 MySQL 的同步任务为例，参见 [创建同步任务](https://docs.pingcap.com/zh/tidb/stable/manage-ticdc#%E5%88%9B%E5%BB%BA%E5%90%8C%E6%AD%A5%E4%BB%BB%E5%8A%A1)。

```shell
cdc cli changefeed create --pd=http://10.0.10.25:2379 --sink-uri="mysql://root:123456@127.0.0.1:3306/" --changefeed-id="simple-replication-task"
```

以下为 TiCDC Dashboard 监控说明 ：

- Server ： TiDB 集群中 TiKV 节点和 TiCDC 节点的概要信息
- Changefeed ： TiCDC 同步任务的详细信息
- Events ： TiCDC 内部数据流转的详细信息
- TiKV ： TiKV 中和 TiCDC 相关的详细信息

![TiCDC Dashboard - Overview](/docs/media/ticdc-dashboard-overview.png)
## Server

- Uptime ： TiKV 节点和 TiCDC 节点已经运行的时间
- Goroutine count ： TiCDC 节点 Goroutine 的个数
- Open FD count ： TiCDC 节点打开的文件句柄个数
- Ownership ： TiCDC 集群中节点的当前状态
- Ownership history ： TiCDC 集群中 Owner 节点的历史记录
- CPU usage ： TiCDC 节点使用的 CPU
- Memory usage ： TiCDC 节点使用的内存
- Etcd health check duration ：TiCDC 节点访问 PD 的延迟统计

![TiCDC Dashboard - Server metrics](/docs/media/ticdc-dashboard-server.png)
## Changefeed

- Changefeed table count ： 一个同步任务中分配到各个 TiCDC 节点同步的数据表个数
- Processor resolved ts ： TiCDC 节点内部状态中已同步的时间点
- Table resolved ts ： 同步任务中各数据表的同步进度
- Changefeed checkpoint ： 同步任务同步到下游的进度，正常情况下绿柱应和黄线相接
- PD etcd requests/s ： TiCDC 节点每秒读写 PD 的次数
- Exit error count ： 每分钟导致同步中断的错误的发生次数
- Changefeed checkpoint lag ： 同步任务上下游数据的进度差（以时间计算）
- Changefeed resolved ts lag ： TiCDC 节点内部同步状态与上游的进度差（以时间计算）
- Flush sink duration ： TiCDC 异步刷写下游的耗时直方图
- Flush sink duration percentile： 每秒钟中 95%，99% 和 99.9% 的情况下，TiCDC 异步刷写下游所花费的时间
- Sink write duration ： TiCDC 将一个事务的更改写到下游的耗时直方图
- Sink write duration percentile： 每秒钟中 95%，99% 和 99.9% 的情况下，TiCDC 将一个事务的更改写到下游所花费的时间
- MySQL sink conflict detect duration ： MySQL 写入冲突检测耗时直方图
- MySQL sink conflict detect duration percentile ： 每秒钟中 95%，99% 和 99.9% 的情况下，MySQL 写入冲突检测耗时
- MySQL sink worker load ： TiCDC 节点中写 MySQL 线程的负载情况

![TiCDC Dashboard - Changefeed metrics 1](/docs/media/ticdc-dashboard-changefeed-1.png)
![TiCDC Dashboard - Changefeed metrics 2](/docs/media/ticdc-dashboard-changefeed-2.png)
![TiCDC Dashboard - Changefeed metrics 3](/docs/media/ticdc-dashboard-changefeed-3.png)

## Events

- Eventfeed count ： TiCDC 节点中 Eventfeed RPC 的个数
- Event size percentile ： 每秒钟中 95% 和 99.9% 的情况下，TiCDC 收到的来自 TiKV 的数据变更消息大小
- Eventfeed error/m ： TiCDC 节点每分钟 Eventfeed RPC 遇到的错误个数
- KV client receive events/s ： TiCDC 节点中 KV client 模块每秒收到来自 TiKV 的数据变更个数
- Puller receive events/s ： TiCDC 节点中 Puller 模块每秒收到来自 KV client 模块的数据变更个数
- Puller output events/s ： TiCDC 节点中 Puller 模块每秒输出到 Sorter 模块的数据变更个数
- Sink flush rows/s ： TiCDC 节点每秒写到下游的数据变更的个数
- Puller buffer size ： TiCDC 节点中缓存在 Puller 模块中的数据变更个数
- Entry sorter buffer size ： TiCDC 节点中缓存在 Sorter 模块中的数据变更个数
- Sink/Mounter buffer size ： TiCDC 节点中缓存在 Buffer Sink 模块和 Mounter 模块中的数据变更个数
- Sink row buffer size ： TiCDC 节点中缓存在 Sink 模块中的数据变更个数
- Entry sorter sort duration ： TiCDC 节点排序数据变更的耗时直方图
- Entry sorter sort duration percentile ： 每秒钟中 95%，99% 和 99.9% 的情况下，TiCDC 排序数据变更所花费的时间
- Entry sorter merge duration ： TiCDC 节点合并排序后的数据变更的耗时直方图
- Entry sorter merge duration percentile ： 每秒钟中 95%，99% 和 99.9% 的情况下，TiCDC 合并排序后的数据变更所花费的时间
- Mounter unmarshal duration ： TiCDC 节点解码的数据变更的耗时直方图
- Mounter unmarshal duration percentile ： 每秒钟中 95%，99% 和 99.9% 的情况下，TiCDC 解码数据变更所花费的时间
- KV client dispatch events/s ： TiCDC 节点内部 KV client 模块每秒分发数据变更的个数
- KV client batch resolved size ： TiKV 批量发给 TiCDC 的 resolved ts 消息的大小

![TiCDC Dashboard - Events metrics 2](/docs/media/ticdc-dashboard-events-1.png)
![TiCDC Dashboard - Events metrics 2](/docs/media/ticdc-dashboard-events-2.png)
![TiCDC Dashboard - Events metrics 2](/docs/media/ticdc-dashboard-events-3.png)

## Unified Sorter
- Unified Sorter intake rate: Unified Sorter 从 puller 消费消息的速率 (events / s)
- Unified Sorter event output rate: Unified Sorter 排序后的消息的输出速率 (events / s)
- Unified Sorter on disk data size: Unified Sorter 暂存在硬盘上的数据大小 (bytes)
- Unified Sorter in-memory data size: Unified Sorter 暂存在内存中的数据大小 (bytes)
- Unified Sorter flush sizes: Unified Sorter 的堆排序器 (heapSorter) 的单次排序操作处理的消息数量 (events)
- Unified Sorter merge size: Unified Sorter 的归并器 (merger) 的单批次中处理的消息数量 (events)
- Unified Sorter resolved ts: Unified Sorter 输出过的最大的 resolved ts, 每个 capture 多个 table 取最小值, 提示排序进度。


## TiKV

- CDC endpoint CPU ： TiKV 节点上 CDC endpoint 线程使用的 CPU
- CDC worker CPU ： TiKV 节点上 CDC worker 线程使用的 CPU
- Min resolved ts ： TiKV 节点上最小的 resolved ts
- Min resovled region ： TiKV 节点上最小的 resolved ts 的 region ID
- Resolved ts lag duration percentile ： TiKV 节点上最小的 resolved ts 与当前时间的差距
- Initial scan duration ： TiKV 节点与 TiCDC 建立链接是增量扫的耗时直方图
- Initial scan duration percentile ： 每秒钟中 95%，99% 和 99.9% 的情况下，TiKV 节点增量扫的耗时
- Memory without block cache ： TiKV 节点在减去 RocksDB block cache 后使用的内存
- CDC pending bytes in memory ： TiKV 节点中 CDC 模块使用的内存
- Captured region count ： TiKV 节点上捕获数据变更的 region 个数

![TiCDC Dashboard - TiKV metrics 1](/docs/media/ticdc-dashboard-tikv-1.png)
![TiCDC Dashboard - TiKV metrics 2](/docs/media/ticdc-dashboard-tikv-2.png)
