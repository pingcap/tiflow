# 自适应 GC safepoint 设计与实现

- Author(s): [shafreeck](https://github.com/shafreeck) (Yongquan Ren)
- Last updated: 2020-04-13

本文来源于[自适应 GC safepoint 设计](./2020-04-13-ticdc-auto-gc-safepoint-1-cn.md)一文，是对其的扩充与讨论。由于在实现的过程中，对于原有的方案设计和实现思路，大家还存在一些分歧，因此特地撰述此文，阐明观点，列出分歧，以供大家讨论。

## 背景

GC safepoint 是通知 TiKV 对 MVCC 数据做版本清理的机制，随着对同一个 Key 的修改，数据库中同一个 Key 会产生多个版本，并用时间戳标识，GC safepoint 记录一个时间戳，允许数据库清理掉小于 safepoint 版本的数据，以避免版本过多带来的数据膨胀。

在 TiKV 原有的设计中，safepoint 主要由 TiDB 维护和管理，本质上是由 TiKV 的 SDK 端进行 GC safepoint 的管理。TiDB 定时更新存储于 PD 的 GC safepoint，TiKV 根据 TiDB 汇报的数据进行数据清理。这种方式，在引入 TiCDC 之前是没有问题的。TiCDC 是一个数据变更捕获服务，用于实时监听 TiDB 集群中数据的变更事件，比如插入、更新等事件。TiCDC 通过扫描 TiKV 数据的历史版本来重放变更事件，而这个历史版本受到 GC safepoint 的影响，即 TiCDC 只能获取到 safepoint 之后的数据变更记录。在 TiCDC 进行数据变更捕获时，如果 TiCDC 暂时不可用，或被用户暂停了任务，下次启动时，历史版本可能已经被删除，导致 TiCDC 无法“断点续传”。

[自适应 GC safepoint 设计](./2020-04-13-ticdc-auto-gc-safepoint-1-cn.md)一文，设计了一种多个服务组件相互协调的机制，来共同决定 GC safepoint 的值，其引入了 GC safepoint limiter 的概念，每个服务可以设置一个对 safepoint 的限制，PD 从所有服务的 limiter 以及 TiDB 的 safepoint 中，选择最小的值，作为 TiKV 实际进行 GC 的值。详细信息，请参考上述文档。

## 设计

### TiDB 与其他组件对等化

在[自适应 GC safepoint 设计](./2020-04-13-ticdc-auto-gc-safepoint-1-cn.md)中，safepoint 的维护依赖于两个概念，TiDB 的 safepoint，以及除了 TiDB 以外其他服务的 safepoint limiter。PD 在决定实际 safepoint 时，取 `real safepoint = min(safepoint, limit1, limit2, limit3…)`。

仔细思考，不难看出，safepoint 和 safepoint limiter 本质上是同一个概念，即某个服务可以接受的 safepoint，比如，TiDB 接受 `safepoint = 10`， TiCDC 接受 `safepoint = 8`，最终允许 TiKV 清理的数据版本为 `ts < 8`。

因此 safepoint 和 safepoint limiter 可以合并统一为 safepoint。从 safepoint 的概念上讲， TiDB 跟 TiCDC 是完全对等的，无需特殊化处理。

### 维持接口基本原语

在[自适应 GC safepoint 设计](./2020-04-13-ticdc-auto-gc-safepoint-1-cn.md) 中，需要新增接口 SetGCSafePointLimit，从上一小节的讨论结论可以看到，limiter 的概念是可以省略掉的，因此，实际上不需要新增接口。这一小节，从另外一个角度论证，无需新增接口，复用原有的 UpdateGCSafePoint 即可。

UpdateGCSafePoint 是用来更新 PD 中保存的 safepoint，其接收一个新的 safepoint 作为参数，并返回更新后的 safepoint 的值。之所以还需要返回 safepoint，是因为如果新的 safepoint 小于当前的记录值，则数据不会被更新，且接口返回旧的 safepoint。

SetGCSafePointLimit 更新某个服务的 safepoint limit，其参数为 service_id，safepoint 以及 ttl，service_id 是用户提供的服务标识，ttl 为这个 safepoint limit 设置一个过期时间，以避免服务挂掉后，数据长期遗留下来。

可以看到，SetGCSafePointLimit 的参数中，设置 service_id 为空，TTL 为 0，则实际上就是 UpdateGCSafePoint 接口的语义，即 UpdateGCSafePoint 只是 SetGCSafePointLimit 的特殊形式。

**因此，本文建议为 UpdateGCSafePoint 新增 service_id，ttl 两个参数，不需要新增额外接口。**

### Delete range 引起的矛盾

TiDB 实际上在汇报 GC safepoint 之前，会调用 delete range 删除因 drop table 等操作而遗留的数据，delete range 成功后，TiDB 才会将 safepoint 更新到 PD。

引入多个 safepoint 之后，TiDB 在 delete range 之前，不能仅依赖自身决定的 safepoint 值，还需要确保小于其他服务设置的 safepoint 值。因此，TiDB 需要先获取系统中除了自己以外的，其他服务的 safepoint 的值。

从上述两节可以看到，我们消除了 limiter 的概念，因此 TiDB 通过 GetGCSafePoint 接口获取到的值，是系统最小值，这个最小值可能是 TiDB 自己维护的，这跟原来的实现有些差异：

假设 TiDB 设置 `safepoint = 100`， TiCDC 设置 `safepoint = 150`，TiDB 更新下次 `safepoint = 200` 之前，执行 delete range 操作，因此需要从 PD 中获取一个合适的 safepoint，即 `safepoint = 150`。但是，如果按现在的设计，系统中的最小值是 100，则跟原来的行为逻辑不符了。这是消除 limiter 的方案带来的最大争议。

这里，分三个层面解释

1. 在上述情况中，TiDB 如果获取到的 `safepoint = 100`，则 100 ~ 150 之间的数据不能通过 delete range 删除掉，但在下一轮 GC 的时候，系统返回最小 `safepoint = 150`，这时 100 ~ 150 之间的数据自然会被清除掉。因此，这种情况只是推迟了数据清除的时间，不影响正确性。
2. 稍微修改一下上述场景，假设TiCDC 设置 `safepoint = 100`， TiDB 设置 `safepoint = 150`，在 TiDB 更新下次 GC safepoint 时，从系统中获取到的 `safepoint = 100`，仍然无法 delete range 删除 100 ~ 150 之间的数据。因此， 1 只是 2 的特殊情况而已。
3. TiDB 在汇报 GC safepoint 之前，调用 delete range 删除数据，这本身违反了 safepoint 的定义，使得大于等于 safepoint 的数据也可能被删除，是一种不严谨的实现，这个实现在只有 TiDB 的时候可能没有问题，但随着其他组件也依赖 safepoint，维护其语义的正确性也变得重要了起来。因此，强烈建议将 GC 的流程改为先更新 safepoint 再 delete range。

### 兼容中心化 GC

UpdateGCSafePoint 除了用来更新 GC safepoint 的值，另外一个作用是通知 TiKV 进行分布式 GC，TiKV 根据 safepoint 的值是否有变化，来决定是否执行 GC，因此，如果 TiDB 不更新这个值，则相当于没有启用分布式 GC。此时，TiDB 可以选择使用旧的 GC 方式。

本文档提供的方案，第三方服务也可以调用 UpdateGCSafePoint 更新对应服务的 safepoint，如果用户在 TiDB 关闭了分布式 GC，则可能导致分布式 GC 和 TiDB 的中心化 GC 同时在执行，引起不必要的资源消耗。

原有的 TiDB 中心化 GC 方案，并没有考虑第三方服务存在的情况，要想兼容这种情况，需要一种解耦的方式，将 TiDB 和第三方服务关联起来。TiDB 需要知道自己的 safepoint 不一定是全局的 safepoint，因此，TiDB 更新 safepoint 后，必须获取到全局 safepoint。获取到全局 safepoint 后， TiDB 执行两种决策：触发分布式 GC，或执行中心化 GC。

因此操作过程如下：

1. TiDB 更新 safepoint 并获取到全局 safepoint，调用 UpdateServiceGCSafepoint 实现
2. TiDB 决策执行分布式 GC，调用 UpdateGCSafePoint
3. TiKV 调用 GetGCSafePoint，判断是否需要 GC

## 实现（PD）

### 新增 UpdateServiceGCSafePoint 接口

```
message UpdateGCSafePointRequest {
  RequestHeader header = 1;
  uint64 safe_point = 2;
  bytes service_id = 3;
  int64 TTL = 4;
}
```

数据保存路径为 `{prefix}/safe_point/service/{service_id}`，TTL 为数据的存活时长。

### 维护 safepoint 最小值

```golang
func (s *Server) UpdateServiceGCSafePoint(ctx context.Context, request pdpb.UpdateGCSafePointRequest) (pdpb.UpdateGCSafePointResponse, error) {

    ...

    serviceID := string(request.ServiceId)
    safePoint := request.SafePoint
    ttl := request.TTL
    minSafePoint := atomic.LoadUint64(&s.minSafePoint)

    // Only save the safe point if it's greater than the previous one
    if safePoint >= minSafePoint {
        if err := s.storage.SaveGCSafePoint(serviceID,
            safePoint, ttl); err != nil {
            return nil, err
        }

        ...

        minSafePoint, err := s.storage.LoadGCSafePoint()
        if err != nil {
            return nil, err
        }
        atomic.StoreUint64(&s.minSafePoint, minSafePoint)
    } else if safePoint < minSafePoint {
        log.Warn("trying to update gc safe point",
            zap.Uint64("min-safe-point", minSafePoint),
            zap.Uint64("safe-point", safePoint))
    }

    return &pdpb.UpdateGCSafePointResponse{
        Header:    s.header(),
        NewSafePoint: minSafePoint,
    }, nil
}
```

这里，相比以前的实现，在获取 safepoint 的时候省掉了一次 etcd 的读取，更新的时候多了一次 etcd 的读取，因此，访问频率相同。需要注意的是，新的方案中，在 LoadGCSafePoint 时需要扫描所有的 Key 并选择出最小值。相对于原来的实现，有了更多的性能消耗。

可以通过内存中维护所有的 safepoint 来优化掉扫描 etcd 的操作，但考虑到以下两点原因，这个优化并没有引入：

- 内存结构需要支持 TTL，引入了一定的实现复杂度
- 更新 safepoint 的频率非常低，以分钟记，服务列表也不会很多，因此性能上完全可以接受

## 总结

本文基于《[自适应 GC safepoint 设计](./2020-04-13-ticdc-auto-gc-safepoint-1-cn.md)》中的设计思路做了精简，消除了 limiter 的概念，并讨论了因此而导致的跟原有 delete range 行为不符的地方。最后，给出了具体的实现方法，并给出了对性能和实现复杂度的取舍。
