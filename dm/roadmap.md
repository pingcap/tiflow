# Data Migration Roadmap

## Primary Focus

- Make DM stable to use: all bugs with [`severity/critical`](https://github.com/pingcap/dm/labels/severity%2Fcritical), [`severity/major`](https://github.com/pingcap/dm/issues?q=is%3Aissue+is%3Aopen+label%3Aseverity%2Fmajor) or [`severity/moderate`](https://github.com/pingcap/dm/issues?q=is%3Aissue+is%3Aopen+label%3Aseverity%2Fmoderate) label must be fixed, and it's better to fix bugs with [`severity/minor`](https://github.com/pingcap/dm/issues?q=is%3Aissue+is%3Aopen+label%3Aseverity%2Fminor) label too.
- Make DM easy to use: continue to improve usability and optimize user experience.

## Usability Improvement

- [ ] bring relay log support back in v2.0 [#1234](https://github.com/pingcap/dm/issues/1234)
  - What: binlog replication unit can read binlog events from relay log, as it did in v1.0
  - Why:
    - AWS Aurora and some other RDS may purge binlog ASAP, but full dump & import may take a long time
    - some users will create many data migration tasks for a single upstream instance, but it's better to avoid pull binlog events many times
- [ ] support to migrate exceeded 4GB binlog file automatically [#989](https://github.com/pingcap/dm/issues/989)
  - What: exceeded 4GB binlog file doesn't interrupt the migration task
  - Why: some operations (like `DELETE FROM` with large numbers of rows, `CREATE TABLE new_tbl AS SELECT * FROM orig_tbl`) in upstream may generate large binlog files
- [ ] better configuration file [#775](https://github.com/pingcap/dm/issues/775)
  - What: avoid misusing for configuration files
  - Why: many users meet problem when write configuration file but don’t know how to deal with it
- [ ] solve other known usability issues (continuous work)
  - What: solve usability issues recorded in [the project](https://github.com/pingcap/dm/projects/3)
  - Why: a lot of usability issues that have been not resolved yet, we need to stop user churn

## New features

- [ ] stop/pause until reached the end of a transaction [#1095](https://github.com/pingcap/dm/issues/1095)
  - What: replicate binlog events until reached the end of the current transaction when stopping or pausing the task
  - Why: achieve transaction consistency as the upstream MySQL after stopped/paused the task
- [ ] stop at the specified position/GTID [#348](https://github.com/pingcap/dm/issues/348)
  - What: stop to replicate when reached the specified binlog position or GTID, like `until_option` in MySQL
  - Why: control over the data we want to replicate more precisely
- [ ] update source config online [#1076](https://github.com/pingcap/dm/issues/1076)
  - What: update source configs (like upstream connection arguments) online
  - Why: switch from one MySQL instance to another in the replica group easier
- [ ] provides a complete online replication checksum feature [#1097](https://github.com/pingcap/dm/issues/1097)
  - What:
    - check data without stopping writing in upstream
    - no extra writes in upstream
  - Why: found potential inconsistency earlier
- [ ] support DM v2.0 in TiDB Operator [tidb-operator#2868](https://github.com/pingcap/tidb-operator/issues/2868)
  - What: use TiDB-Operator to manage DM 2.0
- [ ] use [Lightning](https://github.com/pingcap/tidb-lightning/) to import full dumped data [#405](https://github.com/pingcap/dm/issues/405)
  - What: use Lighting as the full data load unit
  - Why:
    - Lightning is stabler than current Loader in DM
    - Lightning support more source data formats, like CSV
    - Lightning support more storage drivers, like AWS S3

## Performance Improvement

- [ ] flush incremental checkpoint asynchronously [#605](https://github.com/pingcap/dm/pull/605)
  - What: flush checkpoint doesn't block replication for DML statements
  - Why: no block or serialization for DML replication should get better performance

## Out of Scope currently

- Only supports synchronization of MySQL protocol Binlog to TiDB cluster but not all MySQL protocol databases
  - Why: some MySQL protocol databases are not binlog protocol compatible
- Provides a fully automated shard DDL merge and synchronization solution
  - Why: many scenes are difficult to automate
- Replicates multiple upstream MySQL sources with one dm-worker
  - Why: Large amount of development work and many uncertainties
