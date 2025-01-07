

This folder contains all tests which relies on external service such as TiDB.

## Preparations

1. The following seven executables must be copied or linked into these locations:

   - `bin/pd-server`
   - `bin/tikv-server`
   - `bin/tidb-server`
   - `bin/sync_diff_inspector`
   - `bin/dumpling`
   - `bin/loader`
   - `bin/importer`

2. The following programs must be installed:

   - `mysql`(the CLI client)
   - `mysqladmin`

3. The user executing the tests must have permission to create the folder

   `/tmp/tidb_tools_test`. All test artifacts will be written into this folder.

## Running

Run `make integration_test` to execute the integration tests. This command will

1. Build binaries.
2. Check that all executables exist.
3. Execute `tests/run.sh`

If the first two steps are done before, you could also run `tests/run.sh` directly.

The scrip will find out all `tests/*/run.sh` and run it.
