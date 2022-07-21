# Continuous Data Validation

## Background

TiDB already has [`sync-diff-inspector`](https://docs.pingcap.com/tidb/stable/sync-diff-inspector-overview) to do data validation for full data migration, but for incremental data migration there's no such option. We can do full data validation using `sync-diff-inspector` during database downtime for maintenance to validate all incremental data migrated since last full data validation, but its time window is too short for `sync-diff-inspector` to completed and it might be too late to find any incorrect-migrated rows. We need an alternative to do incremental data validation which can validate incremental data in a more real-time way so that we can find incorrect-migrated rows earlier and has less pressure on upstream and downstream database where during validation business queries can run normally.

## Limition

Some known limitions to this design:
- table which needs validation should have primary key or non-null unique key
- there should only be compatible DDL operations in binlog stream between current syncer location and validation location, to make sure validation to use current DDL in schema-tracker to validate row change in history binlog:
  - there is no operation to change primary key or non-null unique key
  - there is no column order change operation
  - there is no drop column operation
  - table in downstream which needs validation cannot be dropped
- do not support validation for tasks which enable extend-column
- do not support validation for tasks which enable event filtering by expression
- TiDB implements floating point data types differently with MySQL, we take it as equal if its absolute difference < 10^-6
- do not support validation for JSON and binary data types

## Detailed Design

### Life cycle of validator

Validator can be enabled together with the task or enabled on the fly, the task which enables validator should has an incremental migration unit, i.e. syncer. Validator will be deleted when the task is deleted. Enabled validator will be in `running` state, it can be stopped manually or stopped when meeting error that cannot be recovered automatically, subsequently turning to `stopped` state. Validator in `stopped` state can be started again and return to `running` state.

### Validation process

1. Validator in `running` state will pull binlog from upstream and get `row change`s.
    - validator will only validate `row change` which has already migrated by syncer.
2. After routing and filtering using the same rule as in syncer, `row change` is dispatched to validator `worker`. `row change` of same table and primary key will be dispatched to the same `worker`.
3. `worker` merges `row change` by table and primary key, we will use the last `row change` meet since last change override previous one. Then put it into `pending row changes`.
4. After accumulates enough `row change`s or after some interval, `worker` queries downstream for data using table and primary key info in those `row change`s, then compare with data in `row change`s
    - for `insert`/`update` type of row change, we validate them differently by validation mode
      - in `full` validation mode, we compare them column by column
      - in `fast` validation mode, we only check it's existence
    - for `delete` type of `row change`, downstream should not contains that data.
5. For `row change`s which are validated successfully, worker will remove it from `pending row changes`, others which fail to validate will be marked as `failed row change` and be validated again after some interval.
6. If a `failed row change` keeps failing to validate for enough time(`delay time`), we mark it as `error row change` and save it into meta database.

### False positive

`error row change` produced in validation process might not be data which is incorrectly migrated, there are cases where `row change` is marked as falsely. Suppose some row which keeps changing on upstream for a time period > `delay time`. If it's marked as `failed row change` since the first time it changes, validator may mark it as `error row change` falsely. In real world scenarios, it's not common.

To reduce the chance of false positive, validator only starts marking `failed row change` when validator has reached the progress of syncer or after some `initial delay time`


### Validation checkpoint

Validator will start validation from previous location after failover or resuming. Validator will save current location and current `pending row changes` into meta-db after some interval.

### Self-protection

Validator `worker` will cache `pending row changes` in memory, to avoid potential OOM, we add an self-protect mechaniam. If there's too much `pending row changes` or the overall size of `pending row changes` is too large, validator will be stopped automatically.

## Concepts

- `row change`: validator decode upstream binlog into `row change` which contains:
    - change type(`insert`/`update`/`delete`)
    - table name
    - data before change(missing for `insert` change)
    - data after change(missing for `delete` change).
- `failed row change`: `row change` which has failed to validate but hasn't been marked as `error row change`
- `error row change`: if some `failed row change` keeps failing to validator for enough time(`delay time`), it's marked as `error row change`