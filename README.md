# ticdc

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_cdc_master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_cdc_master/)
[![codecov](https://codecov.io/gh/pingcap/ticdc/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/ticdc)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/ticdc/badge.svg)](https://coveralls.io/github/pingcap/ticdc)
[![LICENSE](https://img.shields.io/github/license/pingcap/ticdc.svg)](https://github.com/pingcap/ticdc/blob/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/ticdc)](https://goreportcard.com/report/github.com/pingcap/ticdc)

**ticdc** is a change data capture for TiDB, it supports to replicate change data to various downstreams, including MySQL protocol compatible database, message queue via open CDC protocol and other systems such as local file storage.

## Architecture

<img src="docs/media/cdc_architecture.svg?sanitize=true" alt="architecture" width="600"/>

## Documentation

[Chinese Document](https://docs.pingcap.com/zh/tidb/dev/ticdc-overview/)

[English Document](https://pingcap.com/docs/dev/reference/tools/ticdc/overview/)

## Building

To check the source code, run test cases and build binaries, you can simply run:

```bash
$ make
$ make test
```

Notice ticdc supports building with Go version `Go >= 1.13`

When ticdc is built successfully, you can find binary in the `bin` directory. Instructions for unit test and integration test can be found in [Running tests](tests/README.md).

## Deployment

You can setup a CDC cluster for replication test manually as following:

1. setup a TiDB cluster.
2. start a CDC cluster, which contains one or more CDC servers. The command to start on CDC server is `cdc server --pd http://10.0.10.25:2379`, where `http://10.0.10.25:2379` is the client-url of pd-server.
3. start a replication changefeed by `cdc cli changefeed create --pd http://10.0.10.25:2379 --start-ts 413105904441098240 --sink-uri mysql://root:123456@127.0.0.1:3306/`. The tso is TiDB `timestamp oracle`, if it is not provided or set to zero, the tso of start time will be used. Currently we support MySQL protocol compatible database as downstream sink only, we will add more sink type in the future.

## Quick start

```sh
# Start TiDB cluster
$ docker-compose -f docker-compose.yml up -d

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
