# TiFlow

[![LICENSE](https://img.shields.io/github/license/pingcap/tiflow.svg)](https://github.com/pingcap/tiflow/blob/master/LICENSE)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/pingcap/tiflow)
![GitHub Release Date](https://img.shields.io/github/release-date/pingcap/tiflow)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pingcap/tiflow)
[![Build Status](https://github.com/pingcap/tiflow/actions/workflows/check_and_build.yaml/badge.svg?branch=master)](https://github.com/pingcap/tiflow/actions/workflows/check_and_build.yaml?query=event%3Apush+branch%3Amaster)
[![codecov](https://codecov.io/gh/pingcap/tiflow/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/tiflow)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tiflow)](https://goreportcard.com/report/github.com/pingcap/tiflow)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/pingcap/ticdc)

## Introduction

**TiFlow** is a unified data replication platform for [TiDB](https://docs.pingcap.com/tidb/stable) that consists of two main components: TiDB Data Migration (DM) and TiCDC.

* DM enables full data migration and incremental data replication from MySQL or MariaDB to TiDB.
* TiCDC replicates change data to various downstream systems, such as MySQL protocol-compatible databases and [Kafka](https://kafka.apache.org/).

For more details, see [DM README](./README_DM.md) and [TiCDC README](./README_TiCDC.md).

You can also check out the [DeepWiki documentation](https://deepwiki.com/pingcap/tiflow) for more information.

## License

**TiFlow** is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
