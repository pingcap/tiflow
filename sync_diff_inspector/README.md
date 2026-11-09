# sync-diff-inspector

sync-diff-inspector is a tool for comparing two database's data.

## How to use

```shell
Usage of diff:
  -V, --version                  print version of sync_diff_inspector
  -L, --log-level string         log level: debug, info, warn, error, fatal (default "info")
      --redact-info-log          whether to enable the log redaction (default true)
  -C, --config string            Config file
  -T, --template string          <dm|norm> export a template config file
      --dm-addr string           the address of DM
      --dm-task string           identifier of dm task
      --check-thread-count int   how many goroutines are created to check data (default 4)
      --export-fix-sql           set true if want to compare rows or set to false will only compare checksum (default true)
```

For more details you can read the [config.toml](./config/config.toml), [config_sharding.toml](./config/config_sharding.toml) and [config_dm.toml](./config/config_dm.toml).

Log redaction is enabled by default. Disable it with `--redact-info-log=false` or the top-level TOML setting `redact-info-log = false`. An explicit command-line value overrides the configuration file. When enabled, sensitive log fields are replaced with `?`, including SQL and arguments, row values, chunk and histogram bounds, configuration dumps, column metadata, and comparison errors. SQL text can contain literals from a configured `range`, and column definitions can contain default values, generated expressions, and enum/set literals. Database queries, checkpoints, and generated fix SQL use the original values.

Console and report errors retain native connection and filesystem diagnostics, including addresses and paths, but hide arbitrary annotations and data-bearing messages as `details redacted`. Output and log file paths remain visible so users can locate diagnostics.

Configuration parsing errors are also redacted by default. Use `--redact-info-log=false` on the command line to show their details. Settings in a TOML file cannot be loaded if the file has a syntax error.

## Documents
- `zh`: [Overview in Chinese](https://docs.pingcap.com/zh/tidb/stable/sync-diff-inspector-overview)
- `en`: [Overview in English](https://docs.pingcap.com/tidb/stable/sync-diff-inspector-overview)
