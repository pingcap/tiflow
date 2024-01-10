# TiCDC

**TiCDC** is [TiDB](https://docs.pingcap.com/tidb/stable)'s change data capture framework. It replicates change data to various downstream systems, such as MySQL protocol-compatible databases and [Kafka](https://kafka.apache.org/).

## Architecture

<img src="docs/media/cdc_architecture.svg?sanitize=true" alt="architecture" width="600"/>

See a detailed introduction to [the TiCDC architecture](https://docs.pingcap.com/tidb/stable/ticdc-overview#ticdc-architecture).

## Documentation

- [English](https://docs.pingcap.com/tidb/stable/ticdc-overview)
- [简体中文](https://docs.pingcap.com/zh/tidb/stable/ticdc-overview)

## Blog

- [English](https://pingcap.com/blog/)
- [简体中文](https://cn.pingcap.com/blog/)

## Build

To check the source code, run test cases and build binaries, you can simply run:

```bash
$ make cdc
$ make test
```

Note that TiCDC supports building with the Go version `Go >= 1.21`.

When TiCDC is built successfully, you can find binary in the `bin` directory. Instructions for unit test and integration test can be found in [Running tests](./tests/integration_tests/README.md).

## Deploy

You can set up a CDC cluster for replication test manually as following:

1. Set up a TiDB cluster.
2. Start a CDC cluster, which contains one or more CDC servers. The command to start on CDC server
   is `cdc server --pd http://10.0.10.25:2379`, where `http://10.0.10.25:2379` is the client-url of pd-server.
3. Start a replication changefeed by `cdc cli changefeed create --pd http://10.0.10.25:2379 --start-ts 413105904441098240 --sink-uri mysql://root:123456@127.0.0.1:3306/`. The TSO is TiDB `timestamp oracle`. If it is not provided or set to zero, the TSO of start time will be used. Currently, TiCDC supports MySQL protocol-compatible databases as downstream sinks only, and we will add more sink types in the future.

For details, see [Deploy TiCDC](https://docs.pingcap.com/tidb/stable/deploy-ticdc).

## Quick start

```sh
# Start TiDB cluster
$ docker-compose -f ./deployments/ticdc/docker-compose/docker-compose-mysql.yml up -d

# Attach to control container to run TiCDC
$ docker exec -it ticdc_controller sh

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

We welcome and greatly appreciate contributions. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.
