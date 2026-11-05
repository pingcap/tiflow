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

## Connecting to DM with TLS

Use an `https://` address in `--dm-addr` to connect to a TLS-enabled DM-master.
HTTPS verifies the server certificate and hostname against system roots by default.
For a private CA or mutual TLS, configure `[dm-security]` with `ca-path`,
`cert-path`, and `key-path` in the configuration file. See
[config_dm.toml](./config/config_dm.toml) for an example.
HTTPS alone does not restrict API access; configure `ssl-ca`, `ssl-cert`, `ssl-key`,
and `cert-allowed-cn` on DM-master to require and verify client certificates.

Configuring `[dm-security]` requires an HTTPS endpoint. HTTPS redirects are rejected.
Existing HTTP endpoints remain supported when no DM TLS configuration is supplied;
use HTTPS to protect database credentials and configuration in transit.

## Documents
- `zh`: [Overview in Chinese](https://docs.pingcap.com/zh/tidb/stable/sync-diff-inspector-overview)
- `en`: [Overview in English](https://docs.pingcap.com/tidb/stable/sync-diff-inspector-overview)
