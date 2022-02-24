# TiCDC

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_cdc_multi_branch/job/master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_cdc_multi_branch/job/master)
[![codecov](https://codecov.io/gh/pingcap/tiflow/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/tiflow)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/tiflow/badge.svg)](https://coveralls.io/github/pingcap/tiflow)
[![LICENSE](https://img.shields.io/github/license/pingcap/tiflow.svg)](https://github.com/pingcap/tiflow/blob/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tiflow)](https://goreportcard.com/report/github.com/pingcap/tiflow)

**TiCDC** is [TiDB](https://docs.pingcap.com/tidb/stable)'s change data capture framework. It supports replicating change data to various downstreams, including MySQL protocol-compatible databases, message queues via the open CDC protocol and other systems such as local file storage.

## Architecture

<img src="docs/media/cdc_architecture.svg?sanitize=true" alt="architecture" width="600"/>

See a detailed introduction to [the TiCDC architecture](https://docs.pingcap.com/tidb/stable/ticdc-overview#ticdc-architecture).

## Documentation

- [English](https://docs.pingcap.com/tidb/stable/ticdc-overview)
- [Chinese](https://docs.pingcap.com/zh/tidb/stable/ticdc-overview)

## Blog

- [English](https://pingcap.com/blog/)
- [Chinese](https://pingcap.com/blog-cn/)

## Building

To check the source code, run test cases and build binaries, you can simply run:

```bash
$ make
$ make test
```

Note that TiCDC supports building with Go version `Go >= 1.16`.

When TiCDC is built successfully, you can find binary in the `bin` directory. Instructions for unit test and integration test can be found in [Running tests](tests/README.md).

## Deployment

You can setup a CDC cluster for replication test manually as following:

1. Setup a TiDB cluster.
2. Start a CDC cluster, which contains one or more CDC servers. The command to start on CDC server is `cdc server --pd http://10.0.10.25:2379`, where `http://10.0.10.25:2379` is the client-url of pd-server.
3. Start a replication changefeed by `cdc cli changefeed create --pd http://10.0.10.25:2379 --start-ts 413105904441098240 --sink-uri mysql://root:123456@127.0.0.1:3306/`. The TSO is TiDB `timestamp oracle`. If it is not provided or set to zero, the TSO of start time will be used. Currently, we support MySQL protocol-compatible databases as downstream sinks only, and will add more sink types in the future.

For details, see [Deploy TiCDC](https://docs.pingcap.com/tidb/stable/deploy-ticdc).

## Quick start

```sh
# Start TiDB cluster
$ docker-compose -f docker-compose-mysql.yml up -d

# Attach to control container to run TiCDC
$ docker exec -it ticdc_controller_1 sh

# Start to feed the changes on the upstream tidb, and sink to the downstream tidb
$ ./cdc cli changefeed create --pd http://upstream-pd:2379 --sink-uri mysql://root@downstream-tidb:4000/

# Exit the control container
$ exit

# Load data to the upstream tidb
$ sysbench --mysql-host=127.0.0.1 --mysql-user=root --mysql-port=4000 --mysql-db=test oltp_insert --tables=1 --table-size=100000 prepare

# Check sync progress
$ mysql -h 127.0.0.1 -P 5000 -u root -e "SELECT COUNT(*) FROM test.sbtest1"
```

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiCDC is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
