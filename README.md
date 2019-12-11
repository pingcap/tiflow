# ticdc

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_cdc_master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_cdc_master/)
[![codecov](https://codecov.io/gh/pingcap/ticdc/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/ticdc)
[![LICENSE](https://img.shields.io/github/license/pingcap/ticdc.svg)](https://github.com/pingcap/ticdc/blob/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/ticdc)](https://goreportcard.com/report/github.com/pingcap/ticdc)

**ticdc** is a change data capture for TiDB, it supports to replicate change data to various downstreams, including MySQL protocol compatible database, message queue via open CDC protocol and other systems such as local file storage.

## Architecture

![architecture](./docs/media/cdc_architecture.png)

## Documentation

TODO

## Building

To check the source code, run test cases and build binaries, you can simply run:

```bash
$ make
$ make test
```

Notice ticdc supports building with Go version `Go >= 1.13`

When ticdc is built successfully, you can find binary in the `bin` directory.

## Deployment

You can setup a CDC cluster for replication test manually as following:

1. setup a TiDB cluster.
2. start a CDC cluster, which contains one or more CDC servers. The command to start on CDC server is `cdc server --pd-endpoints http://10.0.10.25:2379`, where `http://10.0.10.25:2379` is the client-url of pd-server.
3. start a replication changefeed by `cdc cli --pd-addr http://10.0.10.25:2379 --start-ts 413105904441098240 --sink-uri root@tcp(127.0.0.1:3306)/test`. The tso is TiDB `timestamp oracle`, if it is not provided or set to zero, the tso of start time will be used. Currently we support MySQL protocol compatible database as downstream sink only, we will add more sink type in the future.

## Contributing
Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiCDC is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
