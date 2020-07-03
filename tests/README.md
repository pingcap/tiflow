## Preparations

1. The following executables must be copied or generated or linked into these locations, `sync_diff_inspector` can be downloaded from [tidb-community-toolkit](https://download.pingcap.org/tidb-community-toolkit-v4.0.0-linux-amd64.tar.gz), `tidb-server` related binaries can be downloaded from [tidb-community-server](https://download.pingcap.org/tidb-community-server-v4.0.0-linux-amd64.tar.gz):

    * `bin/tidb-server` # version >= 4.0.0-rc.1
    * `bin/tikv-server` # version >= 4.0.0-rc.1
    * `bin/pd-server`   # version >= 4.0.0-rc.1
    * `bin/pd-ctl`      # version >= 4.0.0-rc.1
    * `bin/sync_diff_inspector`
    * [bin/go-ycsb](https://github.com/pingcap/go-ycsb)
    * [bin/etcdctl](https://github.com/etcd-io/etcd/tree/master/etcdctl)
    * [bin/jq](https://stedolan.github.io/jq/)

2. The user used to execute the tests must have permission to create the folder /tmp/tidb_cdc_test. All test artifacts will be written into this folder.


## Running

### Unit Test

1. Unit test does not need any dependencies, just running `make unit_test` in root dir of source code, or cd into directory of a test case and run single case via `GO111MODULE=on go test -check.f TestXXX`.

### Integration Test

1. Run `make integration_test_build` to generate TiCDC related binaries for integration test

2. Run `make integration_test` to execute the integration tests. This command will

    1. Check that all required executables exist.
    2. Execute `tests/run.sh`

    > If want to run one integration test case only, just pass the CASE parameter, such as `make integration_test CASE=simple`.

    > There exists some environment variables that you can set by yourself, variable details can be found in [test_prepare](_utils/test_prepare).

    > `MySQL sink` will be used by default, if you want to test `Kafka sink`, please run with `make integration_test CASE=simple kafka`

3. After executing the tests, run `make coverage` to get a coverage report at `/tmp/tidb_cdc_test/all_cov.html`.


## Writing new tests

New integration tests can be written as shell scripts in `tests/TEST_NAME/run.sh`. The script should exit with a nonzero error code on failure.
