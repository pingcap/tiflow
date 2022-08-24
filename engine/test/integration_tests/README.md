## Preparations

The following programs must be installed:

* [docker](https://docs.docker.com/get-docker/)
* [docker-compose](https://docs.docker.com/compose/install/)

Besides, make sure you have run the docker daemon. We recommend that you provide docker with at least 6+ cores and 8G+ memory. Of course, the more resources, the better.

## Run engine integration tests

1. Run `make engine_image` to generate engine related image for integration test.
   Or you can run `make tiflow`, `make tiflow-demo`, `make tiflow-chaos-case` to build necessary binaries in your local environment,
   then run `make engine_image_from_local` to generate image by local binaries.
2. Run `make engine_integration_test` to execute the integration tests. This command will

   1. Check that all required executables exist.
   2. Execute `engine/test/integration_tests/run.sh`

   > If want to run one integration test case only, just pass the CASE parameter, such as `make engine_integration_test CASE=e2e_basic`.
   > If want to run integration test cases from the specified one, just pass the START_AT parameter, such as `make engine_integration_test START_AT=e2e_basic` .
   > You can specify multiple tests to run in CASE, for example: `CASE="e2e_basic e2e_worker_error"`. You can even
   > use `CASE="*"` to indicate that you are running all tests.
   >
3. Check logs in `/tmp/tiflow_engine_test/${TEST_NAME}`

## Writing new tests

New integration tests can be written as shell scripts in `engine/test/integration_tests/TEST_NAME/run.sh`. The script should
exit with a nonzero error code on failure.

## Debug

1. Deploy cluster via debug mode

```bash
configs="../../../deployments/engine/docker-compose/3m3e.yaml ../../../deployments/engine/docker-compose/dm_databases.yaml" 
./run.sh debug ${configs}
```

2. Run test scripts
3. Check logs in `/tmp/tiflow_engine_test/debug`