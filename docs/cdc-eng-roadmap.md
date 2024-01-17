# TiCDC Engineering and Architecture Roadmap

- Author(s): [zhangjinpeng87](https://github.com/zhangjinpeng87)
- Last Updated: 2024-01-17

This is a roadmap of TiCDC project from engineering and architecture aspects. It expressed the intension of how we will improve this project to make TiCDC fit more users' use cases in next 1 to 2 years. The roadmap includes several domains like high availability, stability, performance, cloud native architecture evolution, etc. Notice: all contents list in the roadmap is a intension, we will try our best efforts to make it happen, but it doesn't mean we have a commitment for it, please don't rely on features/capabilities currently TiCDC doesn't have for your projects which have strick timeline requirement, even these features listed in the roadmap.

## Stability & Observability

### Goals

- The replication lag is expected (few seconds) under different workloads and throughput
- It is easy to diagnose issues of the whole lifecycle of changfeeds

### Projects

- Improve stability of TiCDC https://github.com/pingcap/tiflow/issues/10343
- TiDB big transaction doesn't block TiCDC replication
- Adding index desn't block sink to MySQL replication https://github.com/pingcap/tiflow/issues/10267
- Better observability for resolve TS blocked issue
- Simplify the timezone handling to reduce maintenance burden
- Optimize memory usage to reduce OOM risk https://github.com/pingcap/tiflow/issues/10386
- Isolate new created changefeed impact on other changefeeds at initilizing stage

## High Availability

### Goals

- The replication lag is expected under planned operations like rolling restart, scale-in and scale-out TiCDC cluster, etc.
- TiCDC can recover quickly under unexpected sinle node failure, partial network failure, and other temporary partial infra failtures.

### Projects

- TiCDC HA capability assesment

## Multiple Upstream Support

### Goals

- Multiple upstream TiDB clusters can share the same TiCDC cluster to reduce hardware cost and maintenance cost
- The same TiCDC architecture can satisfy On-premise customers, TiDB Cloud Dedicated Tier and TiDB Cloud Serverless Tier.

### Projects

- Remove upstream PD denpendency, store changefeed metadata to meta store
- Throuput threshold for different upstream TiDB Cluster
- TiCDC fetch data change logs from the Unified Log Service to eliminate incremental scan issue and simplify network complexity on cloud enviornment
