# TiDB Data Migration Platform

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_dm_multi_branch/job/master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_dm_multi_branch/job/master)
![GitHub release](https://img.shields.io/github/tag/pingcap/dm.svg)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/dm/badge.svg)](https://coveralls.io/github/pingcap/dm)
[![codecov](https://codecov.io/gh/pingcap/dm/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/dm)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/dm)](https://goreportcard.com/report/github.com/pingcap/dm)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fpingcap%2Fdm.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fpingcap%2Fdm?ref=badge_shield)
[![Discuss in Slack](https://img.shields.io/badge/slack-sig--migrate-4A154B?logo=slack)](https://slack.tidb.io/invite?team=tidb-community&channel=sig-migrate&ref=github_sig)

[**TiDB Data Migration (DM)**](https://docs.pingcap.com/tidb-data-migration/stable) is an integrated data migration task management platform that supports full data migration and incremental data replication from MySQL/MariaDB into [TiDB](https://docs.pingcap.com/tidb/stable). It helps reduce the operations cost and simplify the troubleshooting process.

## Architecture

![architecture](docs/media/dm-architecture.png)

## Documentation

* [Detailed documentation](https://docs.pingcap.com/tidb-data-migration/stable/)
* [简体中文文档](https://docs.pingcap.com/zh/tidb-data-migration/stable/)

## Building

To check the code style and build binaries, you can simply run:

```bash
make build
```

Note that DM supports building with Go version `Go >= 1.16`, and unit test preparation can be found in [Running/Unit Test](tests/README.md#Unit-Test)

If you only want to build binaries, you can run:

```bash
make dm-worker  # build DM-worker

make dm-master  # build DM-master

make dmctl      # build dmctl
```

When DM is built successfully, you can find binaries in the `bin` directory.

## Run tests

Run all tests, including unit tests and integration tests:

See [test/README.md](./tests/README.md) for a more detailed guidance.

```bash
make test
```

## Installing

See the "Deploy" section in [our doc](https://docs.pingcap.com/tidb-data-migration/)

## Config File

See the "Configuration" section in [our doc](https://docs.pingcap.com/tidb-data-migration/stable/config-overview)

## Roadmap

Read the [Roadmap](roadmap.md).

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

Any questions? Let's discuss in [#sig-migrate in Slack](https://slack.tidb.io/invite?team=tidb-community&channel=sig-migrate&ref=github_sig)

## License

DM is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fpingcap%2Fdm.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fpingcap%2Fdm?ref=badge_large)

## More resources

- TiDB blog

    - [English](https://pingcap.com/blog/)
    - [简体中文](https://pingcap.com/blog-cn/)

- TiDB case studies

    - [English](https://pingcap.com/case-studies/)
    - [简体中文](https://pingcap.com/cases-cn/)

