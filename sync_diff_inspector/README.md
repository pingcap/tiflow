# sync-diff-inspector

sync-diff-inspector is a tool for comparing two database's data.

## How to use

```shell
Usage of diff:
  -V, --version                  print version of sync_diff_inspector
  -L, --log-level string         log level: debug, info, warn, error, fatal (default "info")
  -C, --config string            Config file
  -T, --template string          <dm|norm> export a template config file
      --dm-addr string           the address of DM
      --dm-task string           identifier of dm task
      --check-thread-count int   how many goroutines are created to check data (default 4)
      --export-fix-sql           set true if want to compare rows or set to false will only compare checksum (default true)
```

For more details you can read the [config.toml](./config/config.toml), [config_sharding.toml](./config/config_sharding.toml) and [config_dm.toml](./config/config_dm.toml).

## Documents
- `zh`: [Overview in Chinese](https://docs.pingcap.com/zh/tidb/stable/sync-diff-inspector-overview)
- `en`: [Overview in English](https://docs.pingcap.com/tidb/stable/sync-diff-inspector-overview)
