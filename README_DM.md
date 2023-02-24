# TiDB Data Migration Platform

[**TiDB Data Migration (DM)**](https://docs.pingcap.com/tidb/stable/dm-overview) is an integrated data migration task management platform that supports full data migration and incremental data replication from MySQL or MariaDB to [TiDB](https://docs.pingcap.com/tidb/stable). It helps reduce the operations cost and simplify the troubleshooting process.

## Architecture

![architecture](dm/docs/media/dm-architecture.png)

## Documentation

- [English](https://docs.pingcap.com/tidb/stable/dm-overview)
- [简体中文](https://docs.pingcap.com/zh/tidb/stable/dm-overview)

## Build

To check the code style and build binaries, you can simply run:

```bash
make build
```

Note that DM supports building with the Go version `Go >= 1.20`. For unit test preparation, see [Running/Unit Test](dm/tests/README.md#Unit-Test).

If you only want to build binaries, you can run:

```bash
make dm-worker  # build DM-worker

make dm-master  # build DM-master

make dmctl      # build dmctl
```

After DM is built successfully, you can find binaries in the `bin` directory.

## Run tests

Run all tests, including unit tests and integration tests:

See [Tests README](dm/tests/README.md) for a more detailed guide.

```bash
make test
```

## Install

See [DM Quick Start Guide](https://docs.pingcap.com/tidb/stable/quick-start-with-dm) and [Deploy a DM Cluster Using TiUP](https://docs.pingcap.com/tidb/stable/deploy-a-dm-cluster-using-tiup).

## Configuration file

See [DM Configuration File Overview](https://docs.pingcap.com/tidb/stable/dm-config-overview).

## Roadmap

See [Roadmap](dm/roadmap.md).

## Contributing

We welcome and greatly appreciate contributions. See [CONTRIBUTING.md](dm/CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

If you have any questions, let's discuss them on the [TiDB Internals forum](https://internals.tidb.io/).

## More resources

- TiDB blog

  - [English](https://pingcap.com/blog/)
  - [简体中文](https://cn.pingcap.com/blog/)

- TiDB case studies

  - [English](https://www.pingcap.com/customers/)
  - [简体中文](https://cn.pingcap.com/case/)
