

This folder contains all tests which relies on external service such as TiDB.

## Preparations

1. The following six executables must be copied or linked into these locations:

   - `bin/pd-server`
   - `bin/tikv-server`
   - `bin/tidb-server`
   - `bin/sync_diff_inspector`
   - `bin/dumpling`
   - `bin/loader`

   `bin/importer` is built automatically by
   `make sync-diff-inspector-integration_test` from
   `github.com/pingcap/tidb/cmd/importer@release-8.5`.

2. The following programs must be installed:

   - `mysql`(the CLI client)
   - `mysqladmin`

3. The user executing the tests must have permission to create the folder

   `/tmp/sync_diff_inspector_test`. All test artifacts will be written into this folder.

## Running

Run `make sync-diff-inspector-integration_test` to execute the integration
tests. This command will

1. Build `bin/sync_diff_inspector`.
2. Build `bin/importer`.
3. Check that all executables exist.
4. Execute `tests/run.sh`

If the first two steps are done before, you could also run `tests/run.sh` directly.

The scrip will find out all `tests/*/run.sh` and run it.
