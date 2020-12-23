# 自适应 GC safepoint 设计

- Author(s): [overvenus](https://github.com/overvenus) (Taining Shen)
- Last updated: 2020-04-13

## 概述

本文档提出了一种新的 GC safepoint 前进机制，可以根据 TiDB 集群中的组件的要求自适应调整 GC safepoint。

## 问题和设计目标

目前 TiDB 的一些服务（比如 CDC，Mydumper 和 BR）和 GC safepoint 紧密相关，一旦 GC safepoint 前进过快就会导致这些服务的不可用。GC safepoint 也不能前进过慢（默认为当前时间 - 10min），否则就有可能导致 TiKV 堆积过多历史数据，影响到在线业务。可见，当集群中存在 CDC，Mydumper 或者 BR 时，对 GC safepoint 的调整需要在满足这些服务要求的同时尽可能地快。

### 目前的实现

目前 GC 的方案见 [分布式 GC 方案](https://docs.google.com/document/d/1tQVC7QlsfkAO4X-cT4kDjSfUvmEmwSUDxpe0lZBVwyU/edit) 和 [PD 现有接口](https://github.com/pingcap/kvproto/blob/b8bc94dd8a3690423ce8051d4ea0a758aff5d2bf/proto/pdpb.proto)，这里具体说下 GC 的 safepoint 更新的机制：

1. TiDB 中定时触发更新 safepoint
2. TiDB 通过调用 pd.UpdateGCSafepoint 更新保存在 PD 上的 GC safpoint
3. TiKV 定时调用 pd.GetGCSafepoint 获取保存在 PD 上的 GC safepoint
4. TiKV 根据 GC safepoint 删除本地数据

通过观察上述步骤可以发现，

1. GC safepoint 保存了两份，分别在 TiDB 和 PD，但是**真正**会影响到 TiKV 的 GC safepoint 保存在 PD 上。
2. GC safepoint 的前进由 TiDB 驱动，但是 TiDB 没有考虑到其他服务。

了解清楚现状后，我们再来看自适应 GC safepoint。

## 自适应 GC safepoint

整个方案就两点：1. GC safepoint 以 PD 为准，2. PD 提供接口设置对 GC safepoint 的限制。

GC safepoint 以 PD 为准是因为，

1. 尽量靠近现在的实现
2. PD 非常适合存储集群元数据
3. txn kv 在没有 TiDB 情况也能使用

 PD 提供接口设置对 GC safepoint 的限制是因为，

1. 提供一种手段让外部服务按需设置 GC safepoint 的限制

### 细节

PD 添加一个新的 RPC: SetGCSafePointLimit。

```
rpc SetGCSafePointLimit(SetGCSafePointLimitRequest) returns (SetGCSafePointLimitResponse) {}
message SetGCSafePointLimitRequest {
   RequestHeader header = 1;
   // UUID v4
   bytes uuid = 2;
   uint64 safe_point = 3;
   uint64 ttl = 4;
}
message SetGCSafePointLimitResponse {
   ResponseHeader header = 1;
}
```

- UUID 是为了 1. 能让多个服务同时对 GC safepoint 做限制，2. 外部服务可以不断前进自己的 limit。
- TTL 是为了确保在外部异常退出的时不会过分阻塞 GC safepoint 的前进。
- TTL 有最大值的限制，可在 PD 侧配置，如果请求 TTL 超过最大值则回复报错
- PD 在 UpdateSafepoint 时，需要确保前进后的safepoint 满足所有的 limit 要求。
- 如果 SetGCSafePointLimit 请求中的 safe_point 已经小于当前 PD 的 safepoint 则回复报错

新设计完整 GC 的 safepoint 更新的机制：

1. TiDB 中定时触发调用 pd.UpdateGCSafepoint
2. PD 根据各个 limit 尝试前进 GC safepoint
3. TiDB 将 PD 返回的 safepoint 保存到本地（兼容性要求）
4. TiKV 定时调用 pd.GetGCSafepoint 获取保存在 PD 上的 GC safepoint
5. TiKV 根据 GC safepoint 删除本地数据

min(safe_point, limit) 可以通过扩展现有的 GetGCSafepoint RPC 接口获得

```

message GetGCSafePointResponse {
   RequestHeader header = 1;
   uint64 safe_point = 2;
   uint64 min_safe_point_limit = 3;
}
```

### 用法举例

以 CDC 为例，CDC 服务中有一个 checkpoint ts 的概念，它的通常只落后于当前时间 1s~1min 之间，我们需要确保 checkpoint ts > GC safepoint，同时我们预计 CDC 意外停止服务后，最多需要 12 小时恢复服务。那么我们可以每隔 10s 向 PD 发生同下请求

```
message SetGCSafePointLimitRequest {
   bytes uuid = CDC_UUID;
   uint64 safe_point = checkpoint_ts;
   uint64 ttl = 12 * 60 * 60; // 12h
}
```
