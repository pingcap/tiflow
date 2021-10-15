# DM Changelog

All notable changes to this project will be documented in this file.

## [2.0.7] 2021-09-23

### Bug fixes

- Fix the error that binlog event is purged when switching `enable-gtid` in source configuration from `false` to `true` [#2094](https://github.com/pingcap/dm/pull/2094)
- Fix the memory leak problem of schema-tracker [#2133](https://github.com/pingcap/dm/pull/2133)

### Improvements

- Disable background statistic job in schema tracker to reduce CPU consumption [#2065](https://github.com/pingcap/dm/pull/2065)
- Support regular expressions rules for online DDL shadow and trash tables [#2139](https://github.com/pingcap/dm/pull/2139)

### Known issues

[GitHub issues](https://github.com/pingcap/dm/issues?q=is%3Aissue+label%3Aaffected-v2.0.7)

## [2.0.6] 2021-08-13

### Bug fixes

- Fix the issue that the metadata inconsistency between ddl infos and upstream tables in the optimistic sharding DDL mode causes DM-master panic [#1971](https://github.com/pingcap/dm/pull/1971)

### Known issues

[GitHub issues](https://github.com/pingcap/dm/issues?q=is%3Aissue+label%3Aaffected-v2.0.6)

## [2.0.5] 2021-07-30

### Improvements

- Support for filtering certain DML using SQL expressions [#1832](https://github.com/pingcap/dm/pull/1832)
- Add `config import/export` command to import and export cluster sources and tasks configuration files for downgrade [#1921](https://github.com/pingcap/dm/pull/1921)
- Optimize safe-mode to improve replication efficiency [#1920](https://github.com/pingcap/dm/pull/1920)
- Maximize compatibility with upstream SQL_MODE [#1894](https://github.com/pingcap/dm/pull/1894)
- Support upstream using both pt and gh-ost online DDL modes in one task [#1918](https://github.com/pingcap/dm/pull/1918)
- Improve the efficiency of replication of DECIMAL types [#1841](https://github.com/pingcap/dm/pull/1841)
- Support for automatic retry of transaction-related retryable errors [#1916](https://github.com/pingcap/dm/pull/1916)

### Bug fixes

- Fix the issue that the inconsistency of upstream and downstream primary keys might lead to data loss [#1919](https://github.com/pingcap/dm/pull/1919)
- Fix the issue that too many upstream sources cause cluster upgrade failure and DM-master OOM [#1868](https://github.com/pingcap/dm/pull/1868)
- Fix the issue of the configuration item `case-sensitive` [#1886](https://github.com/pingcap/dm/pull/1886)
- Fix the issue that the default value of `tidb_enable_change_column_type` inside DM is wrong [#1843](https://github.com/pingcap/dm/pull/1843)
- Fix the issue that the `auto_random` column in downstream may causes task interruption [#1847](https://github.com/pingcap/dm/pull/1847)
- Fix the issue that `operate-schema set -flush` command causes DM-worker panic [#1829](https://github.com/pingcap/dm/pull/1829)
- Fix the issue that DDL fails to coordinate within DM-worker due to repeated execution of the same DDL in pessimistic mode [#1816](https://github.com/pingcap/dm/pull/1816)
- Fix the issue that wrong configuration causes DM-worker panic [#1842](https://github.com/pingcap/dm/pull/1842)
- Fix the issue that redoing tasks causes loader panic [#1822](https://github.com/pingcap/dm/pull/1822)
- Fix the issue that DM binlog file name is not timely updated after upstream master-slave switch [#1874](https://github.com/pingcap/dm/pull/1874)
- Fix the issue of incorrect value of replication delay monitoring [#1880](https://github.com/pingcap/dm/pull/1880)
- Fix the issue that block-allow-list fails to filter online DDL in some cases [#1867](https://github.com/pingcap/dm/pull/1867)
- Fix the issue that the task cannot be stopped manually due to the error after automatic resuming [#1917](https://github.com/pingcap/dm/pull/1917)


### Known issues

[GitHub issues](https://github.com/pingcap/dm/issues?q=is%3Aissue+label%3Aaffected-v2.0.5)

## [1.0.7] 2021-06-21

### Bug fixes

- Fix the issue that data may be lost after a task restarts from interruption [#1783](https://github.com/pingcap/dm/pull/1783)

## [2.0.4] 2021-06-18

### Improvements

- Support rescheduling and automatically resuming tasks after a DM-worker goes offline first and then comes back online during the full import [#1784](https://github.com/pingcap/dm/pull/1784)
- Add the metric `replicationLagGauge` to monitor replication delay [#1759](https://github.com/pingcap/dm/pull/1759)
- Restore schemas in parallel during the full import [#1701](https://github.com/pingcap/dm/pull/1701)
- Support automatically adjusting the time_zone settings of both the upstream and downstream databases [#1714](https://github.com/pingcap/dm/pull/1714)
- Improve the speed of rolling back incremental replication tasks after the tasks meet errors  [#1705](https://github.com/pingcap/dm/pull/1705)
- Automatically adjust GTID according to checkpoints when GTID is enabled during the incremental replication [#1745](https://github.com/pingcap/dm/pull/1745)
- Detect the versions of upstream and downstream databases and record the versions in log files [#1693](https://github.com/pingcap/dm/pull/1693)
- Use the schema from the dump stage of the full export as the initial schema for the incremental replication task of the same data source [#1754](https://github.com/pingcap/dm/pull/1754)
- Decrease the time that the safe mode lasts after the incremental task is restarted to 1 minute to improve the replication speed [#1779](https://github.com/pingcap/dm/pull/1779)
- Improve the usability of dmctl 
  - Support setting the address of DM-master as an environment variable [#1726](https://github.com/pingcap/dm/pull/1726)
  - Support specifying the `master-addr` parameter anywhere in a dmctl command [#1771](https://github.com/pingcap/dm/pull/1771)
  - Use the `encrypt`/`decrypt` command instead of the `--decrypt`/`-encrypt` parameter to encrypt or decrypt the database password [#1771](https://github.com/pingcap/dm/pull/1771)

### Bug fixes

- Fix the issue that data may be lost after a non-GTID task restarts from interruption [#1781](https://github.com/pingcap/dm/pull/1781)
- Fix the issue that the data source binding information may be lost after upgrading a DM cluster which has been downgraded before [#1713](https://github.com/pingcap/dm/pull/1713)
- Fix the issue that etcd reports that the wal directory does not exist when DM-master restarts [#1680](https://github.com/pingcap/dm/pull/1680)
- Fix the issue that the number of error messages reported from precheck exceeds the grpc limit [#1688](https://github.com/pingcap/dm/pull/1688)
- Fix the issue that DM-worker panics when replicating unsupported statements from a MariaDB database of an earlier version [#1734](https://github.com/pingcap/dm/pull/1734)
- Fix the issue that DM does not update the metric of relay log disk capacity [#1753](https://github.com/pingcap/dm/pull/1753)
- Fix the issue that DM may panic when getting the master status of the upstream database binlog [#1774](https://github.com/pingcap/dm/pull/1774)

### Known issues

[GitHub issues](https://github.com/pingcap/dm/issues?q=is%3Aissue+label%3Aaffected-v2.0.4)

## [2.0.3] 2021-05-10

### Improvements

- Support deleting residual DDL locks using the command `unlock-ddl-lock` after the migration task is stopped [#1612](https://github.com/pingcap/dm/pull/1612)
- Support limiting the number of errors and warnings that DM reports during the precheck process [#1621](https://github.com/pingcap/dm/pull/1621)
- Optimize the behavior of the command `query-status` to get the status of upstream binlogs [#1630](https://github.com/pingcap/dm/pull/1630)
- Optimize the format of sharded tables’ migration status output by the command `query-status` in the pessimistic mode [#1650](https://github.com/pingcap/dm/pull/1650)
- Print help message first when dmctl processes commands with the `--help` input [#1637](https://github.com/pingcap/dm/pull/1637)
- Automatically remove the related information from monitoring panels after a DDL lock is deleted [#1631](https://github.com/pingcap/dm/pull/1631)
- Automatically remove the related task status from monitoring panels after a task is stopped or completed [#1614](https://github.com/pingcap/dm/pull/1614)

### Bug fixes

- Fix the issue that DM-master becomes out of memory after DM is updated to v2.0.2 in the process of shard DDL coordination using the optimistic mode [#1643](https://github.com/pingcap/dm/pull/1643) [#1649](https://github.com/pingcap/dm/pull/1649)
- Fix the issue that the source binding information is lost when DM is started for the first time after updated to v2.0.2 [#1649](https://github.com/pingcap/dm/pull/1649)
- Fix the issue that the flag in the command `operate-source show -s` does not take effect [#1587](https://github.com/pingcap/dm/pull/1587)
- Fix the issue that the command `operate-source stop <config-file>` fails because DM cannot connect to the source [#1587](https://github.com/pingcap/dm/pull/1587)
- Fix the finer-grained issue that some migration errors might be wrongly ignored [#1599](https://github.com/pingcap/dm/pull/1599)
- Fix the issue that the migration is interrupted when DM filters online DDL statements according to binlog event filtering rules that are configured [#1668](https://github.com/pingcap/dm/pull/1668)

### Known issues

[GitHub issues](https://github.com/pingcap/dm/issues?q=is%3Aissue+label%3Aaffected-v2.0.3)

## [2.0.2] 2021-04-09

### Improvements

- Relay log GA
  - The relay log feature is no longer enabled by setting the source configuration file. Now, the feature is enabled by running commands in dmctl for specified DM-workers [#1499](https://github.com/pingcap/dm/pull/1499)
  - DM sends the commands `query-status -s` and `purge-relay` to all DM-workers that pull relay logs [#1533](https://github.com/pingcap/dm/pull/1533)
  - Align the relay unit‘s behavior of pulling and sending binlogs with that of the secondary MySQL database [#1390](https://github.com/pingcap/dm/pull/1390)
  - Reduce the scenarios where relay logs need to be purged [#1400](https://github.com/pingcap/dm/pull/1400)
  - Support sending heartbeat events when the relay log feature is enabled to display task progress with regular updates [#1404](https://github.com/pingcap/dm/pull/1404)
- Optimistic sharding DDL mode
  - Optimize operations for resolving DDL conflicts [#1496](https://github.com/pingcap/dm/pull/1496) [#1506](https://github.com/pingcap/dm/pull/1506) [#1518](https://github.com/pingcap/dm/pull/1518) [#1551](https://github.com/pingcap/dm/pull/1551)
  - Adjust the DDL coordination behavior in the optimistic mode to avoid data inconsistency in advance [#1510](https://github.com/pingcap/dm/pull/1510) [#1512](https://github.com/pingcap/dm/pull/1512)
- Support automatically recognizing the switching of upstream data sources when the source configuration needs no update, for example, when the IP address does not change [#1364](https://github.com/pingcap/dm/pull/1364)
- Precheck the privileges of the upstream MySQL instance at a finer granularity [#1336](https://github.com/pingcap/dm/pull/1366) 
- Support configuring binlog event filtering rules in the source configuration file [#1370](https://github.com/pingcap/dm/pull/1370) 
- When binding an idle upstream data source to an idle DM-worker node, DM-master nodes firstly choose the most recent binding of that DM-worker node [#1373](https://github.com/pingcap/dm/pull/1373)
- Improve the stability of DM automatically getting the SQL mode from the binlog file [#1382](https://github.com/pingcap/dm/pull/1382) [#1552](https://github.com/pingcap/dm/pull/1552)
- Support automatically parsing GTIDs of different formats in the source configuration file [#1385](https://github.com/pingcap/dm/pull/1385)
- Extend DM-worker’s TTL for keepalive to reduce scheduling caused by poor network [#1405](https://github.com/pingcap/dm/pull/1405)
- Support reporting an error when the configuration file contains configuration items that are not referenced [#1410](https://github.com/pingcap/dm/pull/1410)
- Improve the display of a GTID set by sorting it in dictionary order [#1424](https://github.com/pingcap/dm/pull/1424)
- Optimize monitoring and alerting rules [#1438](https://github.com/pingcap/dm/pull/1438)
- Support manually transferring an upstream data source to a specified DM-worker [#1492](https://github.com/pingcap/dm/pull/1492)
- Add configurations of etcd compaction and disk quota [#1521](https://github.com/pingcap/dm/pull/1521)

### Bug fixes

- Fix the issue of data loss during the full data migration occurred because DM frequently restarts the task [#1378](https://github.com/pingcap/dm/pull/1378)
- Fix the issue that an incremental replication task fails to start when the binlog position is not specified together with GTID in the task configuration [#1393](https://github.com/pingcap/dm/pull/1393)
- Fix the issue that DM-worker’s binding relationships become abnormal when the disk and network environments are poor [#1396](https://github.com/pingcap/dm/pull/1396)
- Fix the issue that enabling the relay log feature might cause data loss when the GTIDs specified in upstream binlog `previous_gtids` events are not consecutive [#1390](https://github.com/pingcap/dm/pull/1390) [#1430](https://github.com/pingcap/dm/pull/1430)
- Disable the heartbeat feature of DM v1.0 to avoid the failure of high availability scheduling [#1467](https://github.com/pingcap/dm/pull/1467)
- Fix the issue that the migration fails if the upstream binlog sequence number is larger than 999999 [#1476](https://github.com/pingcap/dm/pull/1476)
- Fix the issue that DM commands hang when DM gets stuck in pinging the upstream and downstream databases [#1477](https://github.com/pingcap/dm/pull/1477)
- Fix the issue that the full import fails when the upstream database enables the `ANSI_QUOTES` mode [#1497](https://github.com/pingcap/dm/pull/1497)
- Fix the issue that DM might duplicate binlog events when the GTID and the relay log are enabled at the same time [#1525](https://github.com/pingcap/dm/pull/1525)

### Known issues

[GitHub issues](https://github.com/pingcap/dm/issues?q=is%3Aissue+label%3Aaffected-v2.0.2)

## [2.0.1] 2020-12-25

### Improvements

- Support the relay log feature in high availability scenarios [#1353](https://github.com/pingcap/dm/pull/1353)
    - DM-worker supports storing relay logs only locally.
    - In scenarios where a DM-worker node is down or is offline due to network fluctuations, the newly scheduled DM-worker pulls the upstream binlog again.
- Restrict the `handle-error` command to only handle DDL errors to avoid misuse [#1303](https://github.com/pingcap/dm/pull/1303)
- Support simultaneously connecting multiple DM-master nodes and automatically switching connected nodes in dmctl [#1349](https://github.com/pingcap/dm/pull/1349)
- Add the `get-config` command to get the configuration of migration tasks and DM components [#1348](https://github.com/pingcap/dm/pull/1348)
- Support migrating SQL statements like `ALTER TABLE ADD COLUMN (xx, xx)` [#1345](https://github.com/pingcap/dm/pull/1345)
- Support automatically filtering SQL statements like `CREATE/ALTER/DROP EVENT` [#1343](https://github.com/pingcap/dm/pull/1343)
- Support checking whether `server-id` is set for the upstream MySQL/MariaDB instance before the incremental replication task starts [#1315](https://github.com/pingcap/dm/pull/1315)
- Support replicating schemas and tables with `sql` in their names during the full import [#1259](https://github.com/pingcap/dm/pull/1259)

### Bug fixes

- Fix the issue that restarting a task might cause `fail to initial unit Sync of subtask` error [#1274](https://github.com/pingcap/dm/pull/1274)
- Fix the issue that the `pause-task` command might be blocked when it is executed during the full import [#1269](https://github.com/pingcap/dm/pull/1269) [#1277](https://github.com/pingcap/dm/pull/1277)
- Fix the issue that DM fails to create a data source for a MariaDB instance when `enable-gtid: true` is configured [#1344](https://github.com/pingcap/dm/pull/1344)
- Fix the issue that the `query-status` command might be blocked when it is executed [#1293](https://github.com/pingcap/dm/pull/1293)
- Fix the issue that concurrently coordinating multiple DDL statements in the pessimistic shard DDL mode might block the task [#1263](https://github.com/pingcap/dm/pull/1263)
- Fix the issue that running the `pause-task` command might get the meaningless `sql: connection is already closed` error [#1304](https://github.com/pingcap/dm/pull/1304)
- Fix the issue that the full migration fails when the upstream instance does not have the `REPLICATION` privilege [#1326](https://github.com/pingcap/dm/pull/1326)
- Fix the issue that the `route-rules` configuration of a shard merge task does not take effect in the full import when the `SQL_MODE` of the task contains `ANSI_QUOTES` [#1314](https://github.com/pingcap/dm/pull/1314)
- Fix the issue that DM fails to automatically apply the `SQL_MODE` of the upstream database during the incremental replication [#1307](https://github.com/pingcap/dm/pull/1307)
- Fix the issue that DM logs the `fail to parse binlog status_vars` warning when automatically parsing the `SQL_MODE` of the upstream database [#1299](https://github.com/pingcap/dm/pull/1299)

## [2.0.0] 2020-10-30

### Improvements

- Optimize the setting of `safe-mode` to ensure the eventual consistency of data when the upstream database, such as Amazon Aurora and Aliyun RDS, does not support FTWRL in the full export [#981](https://github.com/pingcap/dm/pull/981) [#1017](https://github.com/pingcap/dm/pull/1017)
- Support automatic configuration of `sql_mode` for data migration based on the global `sql_mode` of upstream and downstream databases and `sql_mode` of binlog events [#1005](https://github.com/pingcap/dm/pull/1005) [#1071](https://github.com/pingcap/dm/pull/1071) [#1137](https://github.com/pingcap/dm/pull/1137)
- Support automatic configuration of the `max_allowed_packet` from DM to the downstream TiDB, based on the global `max_allowed_packet` value of the downstream TiDB [#1071](https://github.com/pingcap/dm/pull/1071)
- Optimize the incremental replication speed compared with DM 2.0 RC version [#1203](https://github.com/pingcap/dm/pull/1203)
- Improve performance by using optimistic transaction to migrate data to TiDB by default [#1107](https://github.com/pingcap/dm/pull/1107)
- Support DM-worker automatically fetching and using the list of DM-master nodes in the cluster [#1180](https://github.com/pingcap/dm/pull/1180)
- Disable auto-resume behavior for more errors that cannot be automatically recovered [#979](https://github.com/pingcap/dm/pull/979) [#1085](https://github.com/pingcap/dm/pull/1085) [#1216](https://github.com/pingcap/dm/pull/1216)

### Bug fixes

- Fix the issue that failure to automatically set the default value of `statement-size` for full export might cause the `packet for query is too large` error or the OOM issue in TiDB [#1133](https://github.com/pingcap/dm/pull/1133)
- Fix DM-worker panic when there are concurrent checkpoint operations during the full import [#1182](https://github.com/pingcap/dm/pull/1182)
- Fix the issue that the migration task might have `table checkpoint position * less than global checkpoint position` error and be interrupted after the upstream MySQL/MariaDB instance is restarted [#1041](https://github.com/pingcap/dm/pull/1041)
- Fix the issue that migration tasks might be interrupted when the upstream database does not enable GTID [#1123](https://github.com/pingcap/dm/pull/1123)
- Fix the issue that the DM-master node does not start properly after conflicts occur during the shard DDL coordination [#1199](https://github.com/pingcap/dm/pull/1199)
- Fix the issue that the incremental replication might be too slow when there are multiple common indexes in the table to be migrated [#1063](https://github.com/pingcap/dm/pull/1063)
- Fix the issue that the progress display is abnormal after restarting the migration task during the full import [#1043](https://github.com/pingcap/dm/pull/1043)
- Fix the issue that paused migration subtasks cannot be obtained by `query-status` after being scheduled to another DM-worker [#1183](https://github.com/pingcap/dm/pull/1183)
- Fix the issue that `FileSize` might not take effect during the full export [#1191](https://github.com/pingcap/dm/pull/1191)
- Fix the issue that the `-s` parameter in `extra-args` does not take effect during the full export [#1196](https://github.com/pingcap/dm/pull/1196)
- Fix the issue that enabling the online DDL feature might cause `not allowed operation: alter multiple tables in one statement` error [#1192](https://github.com/pingcap/dm/pull/1192)
- Fix the issue that during the incremental replication, the migration task might be interrupted when the DDL statements to be migrated are associated with other tables, such as DDL statements related to foreign keys [#1101](https://github.com/pingcap/dm/pull/1101) [#1108](https://github.com/pingcap/dm/pull/1108)
- Fix the issue that database names and table names with character `/` are not correctly parsed during the full migration [#991](https://github.com/pingcap/dm/pull/991)
- Fix the issue that after failing to migrate DDL statements to the downstream TiDB database during the incremental replication, migration tasks might not be paused and the corresponding error cannot be obtained from `query-status` [#1059](https://github.com/pingcap/dm/pull/1059)
- Fix the issue that concurrently coordinating multiple DDL statements in the optimistic shard DDL mode might block the task [#1051](https://github.com/pingcap/dm/pull/1051)
- Fix the issue that a DM-master might try to forward requests to other DM-master nodes after it becomes the leader [#1157](https://github.com/pingcap/dm/pull/1157)
- Fix the issue that DM cannot parse `GRANT CREATE TABLESPACE` during the precheck [#1113](https://github.com/pingcap/dm/pull/1113)
- Fix the issue that migration tasks are interrupted when migrating `DROP TABLE` statements but corresponding tables don’t exist [#990](https://github.com/pingcap/dm/pull/990)
- Fix the issue that `operate-schema` might not work properly when the `--source` parameter is specified [#1106](https://github.com/pingcap/dm/pull/1106)
- Fix the issue that `list-member` cannot be executed correctly after enabling TLS [#1050](https://github.com/pingcap/dm/pull/1050)
- Fix the issue that mixing `https` and `http` in the config items might cause the cluster to not work properly after enabling TLS [#1220](https://github.com/pingcap/dm/pull/1220)
- Fix the issue that the HTTP API cannot work properly after configuring the `cert-allowed-cn` parameter for DM-masters [#1036](https://github.com/pingcap/dm/pull/1036)
- Fix the issue that for incremental replication tasks, the configuration check fails when `binlog-gtid` is only specified in the `meta` of the task configuration [#987](https://github.com/pingcap/dm/pull/987)
- Fix the issue that in the interactive mode, dmctl cannot correctly execute some commands starting or ending with blank characters [#1202](https://github.com/pingcap/dm/pull/1202)
- Fix the issue that the `converting NULL to string is unsupported` error is output to the log file during the full export [#1014](https://github.com/pingcap/dm/pull/1014)
- Fix the issue that the progress might be displayed as `NaN` during the full import [#1209](https://github.com/pingcap/dm/pull/1209)

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

## [2.0.0-rc.2] 2020-09-01

### Improvements

- Support more AWS Aurora-specific privileges when pre-checking the data migration task [#950](https://github.com/pingcap/dm/pull/950)
- Check whether GTID is enabled for the upstream MySQL/MariaDB when configuring `enable-gtid: true` and creating a data source [#957](https://github.com/pingcap/dm/pull/957)

### Bug fixes

- Fix the `Column count doesn't match value count` error that occurs in the running migration task after automatically upgrading the DM cluster from v1.0.x to v2.0.0-rc [#952](https://github.com/pingcap/dm/pull/952)
- Fix the issue that the DM-worker or DM-master component might not correctly exit [#963](https://github.com/pingcap/dm/pull/963)
- Fix the issue that the `--no-locks` argument does not take effect on the dump processing unit in DM v2.0 [#961](https://github.com/pingcap/dm/pull/961)
- Fix the `field remove-meta not found in type config.TaskConfig` error that occurs when using the task configuration file of the v1.0.x cluster to start the task of a v2.0 cluster [#965](https://github.com/pingcap/dm/pull/965)
- Fix the issue that when the domain name is used as the connection address of each component, the component might not be correctly started [#955](https://github.com/pingcap/dm/pull/955)
- Fix the issue that the connection between the upstream and downstream might not be released after the migration task is stopped [#943](https://github.com/pingcap/dm/pull/943)
- Fix the issue that in the optimistic sharding DDL mode, concurrently executing the DDL statement on multiple sharded tables might block the sharding DDL coordination [#944](https://github.com/pingcap/dm/pull/944)
- Fix the issue that the newly started DM-master might cause the `list-member` to panic [#970](https://github.com/pingcap/dm/pull/970)

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

## [2.0.0-rc] 2020-08-21

### Improvements

- Support high availability for data migration tasks
- Add an optimistic mode for sharding DDL statements
- Add the `handle-error` command to handle errors during DDL incremental replication
- Add a `workaround` field in the error returned by `query-status` to suggest the error handling method
- Improve the monitoring dashboards and alert rules
- Replace Mydumper with Dumpling as the full export unit
- Support the GTID mode when performing incremental replication to the downstream
- Support TLS connections between upstream and downstream databases, and between DM components
- Support the incremental replication scenarios where the table of the downstream has more columns than that of the upstream
- Add a `--remove-meta` option to the `start-task` command to clean up metadata related to data migration tasks
- Support dropping columns with single-column indices
- Support automatically cleaning up temporary files after a successful full import
- Support checking whether the table to be migrated has a primary key or a unique key before starting a migration task
- Support connectivity check between dmctl and DM-master while starting dmctl
- Support connectivity check for downstream TiDB during the execution of `start-task`/`check-task`
- Support replacing task names with task configuration files for some commands such as `pause-task`
- Support logs in `json` format for DM-master and DM-worker components
- Remove the call stack information and redundant fields in the error message returned by `query-status`
- Improve the binlog position information of the upstream database returned by `query-status`
- Improve the processing of `auto resume` when an error is encountered during the full export

### Bug fixes

- Fix the issue of goroutine leak after executing `stop-task`
- Fix the issue that the task might not be paused after executing `pause-task`
- Fix the issue that the checkpoint might not be saved correctly in the initial stage of incremental replication
- Fix the issue that the `BIT` data type is incorrectly handled during incremental replication

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Support high availability for data migration tasks [#473](https://github.com/pingcap/dm/pull/473)
- Add an optimistic mode for sharding DDL statements [#568](https://github.com/pingcap/dm/pull/568)
- Add the `handle-error` command to handle errors during DDL incremental replication [#850](https://github.com/pingcap/dm/pull/850)
- Add a `workaround` field in the error returned by `query-status` to suggest the error handling method [#753](https://github.com/pingcap/dm/pull/753)
- Improve the monitoring dashboards and alert rules [#853](https://github.com/pingcap/dm/pull/853)
- Replace Mydumper with Dumpling as the full export unit [#540](https://github.com/pingcap/dm/pull/540)
- Support the GTID mode when performing incremental replication to the downstream [#521](https://github.com/pingcap/dm/pull/521)
- Support TLS connections between upstream and downstream databases, and between DM components [#569](https://github.com/pingcap/dm/pull/569)
- Support the incremental replication scenarios where the table of the downstream has more columns than that of the upstream [#379](https://github.com/pingcap/dm/pull/379)
- Add a `--remove-meta` option to the `start-task` command to clean up metadata related to data migration tasks [#651](https://github.com/pingcap/dm/pull/651)
- Support dropping columns with single-column indices [#801](https://github.com/pingcap/dm/pull/801)
- Support automatically cleaning up temporary files after a successful full import [#770](https://github.com/pingcap/dm/pull/770)
- Support checking whether the table to be migrated has a primary key or a unique key before starting a migration task [#870](https://github.com/pingcap/dm/pull/870)
- Support connectivity check between dmctl and DM-master while starting dmctl [#786](https://github.com/pingcap/dm/pull/786)
- Support connectivity check for downstream TiDB during the execution of `start-task`/`check-task` [#769](https://github.com/pingcap/dm/pull/769) 
- Support replacing task names with task configuration files for some commands such as `pause-task` [#854](https://github.com/pingcap/dm/pull/854)
- Support logs in `json` format for DM-master and DM-worker components [#808](https://github.com/pingcap/dm/pull/808)
- Remove the call stack information in the error message returned by `query-status` [#746](https://github.com/pingcap/dm/pull/746)
- Remove the redundant fields in the error message returned by `query-status` [#771](https://github.com/pingcap/dm/pull/771)
- Improve the binlog position information of the upstream database returned by `query-status` [#830](https://github.com/pingcap/dm/pull/830)
- Improve the processing of `auto resume` when an error is encountered during the full export [#872](https://github.com/pingcap/dm/pull/872)
- Fix the issue of goroutine leak after executing `stop-task` [#731](https://github.com/pingcap/dm/pull/731)
- Fix the issue that the task might not be paused after executing `pause-task` [#644](https://github.com/pingcap/dm/pull/644)
- Fix the issue that the checkpoint might not be saved correctly in the initial stage of incremental replication [#758](https://github.com/pingcap/dm/pull/758)
- Fix the issue that the `BIT` data type is incorrectly handled during incremental replication [#876](https://github.com/pingcap/dm/pull/876)

## [1.0.6] 2020-06-17

### Improvements

- Support the original plaintext passwords for upstream and downstream databases
- Support configuring session variables for DM’s connections to upstream and downstream databases
- Remove the call stack information in some error messages returned by the `query-status` command when the data migration task encounters an exception 
- Filter out the items that pass the precheck from the message returned when the precheck of the data migration task fails

### Bug fixes

- Fix the issue that the data migration task is not automatically paused and the error cannot be identified by executing the `query-status` command if an error occurs when the load unit creates a table 
- Fix possible DM-worker panics when data migration tasks run simultaneously 
- Fix the issue that the existing data migration task cannot be automatically restarted when the DM-worker process is restarted if the `enable-heartbeat` parameter of the task is set to `true` 
- Fix the issue that the shard DDL conflict error may not be returned after the task is resumed 
- Fix the issue that the `replicate lag` information is displayed incorrectly for an initial period of time when the `enable-heartbeat` parameter of  the data migration task is set to `true` 
- Fix the issue that `replicate lag` cannot be calculated using the heartbeat information when `lower_case_table_names` is set to `1` in the upstream database 
- Disable the meaningless auto-resume tasks triggered by the `unsupported collation` error during data migration

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Support the original plaintext passwords for upstream and downstream databases [#676](https://github.com/pingcap/dm/pull/676)
- Support configuring session variables for DM’s connections to upstream and downstream databases [#692](https://github.com/pingcap/dm/pull/692)
- Remove the call stack information in some error messages returned by the `query-status` command when the data migration task encounters an exception [#733](https://github.com/pingcap/dm/pull/733) [#747](https://github.com/pingcap/dm/pull/747)
- Filter out the items that pass the precheck from the message returned when the precheck of the data migration task fails [#730](https://github.com/pingcap/dm/pull/730)
- Fix the issue that the data migration task is not automatically paused and the error cannot be identified by executing the `query-status` command if an error occurs when the load unit creates a table [#747](https://github.com/pingcap/dm/pull/747)
- Fix possible DM-worker panics when data migration tasks run simultaneously [#710](https://github.com/pingcap/dm/pull/710)
- Fix the issue that the existing data migration task cannot be automatically restarted when the DM-worker process is restarted if the `enable-heartbeat` parameter of the task is set to `true` [#739](https://github.com/pingcap/dm/pull/739)
- Fix the issue that the shard DDL conflict error may not be returned after the task is resumed [#739](https://github.com/pingcap/dm/pull/739) [#742](https://github.com/pingcap/dm/pull/742)
- Fix the issue that the `replicate lag` information is displayed incorrectly for an initial period of time when the `enable-heartbeat` parameter of  the data migration task is set to `true` [#704](https://github.com/pingcap/dm/pull/704)
- Fix the issue that `replicate lag` cannot be calculated using the heartbeat information when `lower_case_table_names` is set to `1` in the upstream database [#704](https://github.com/pingcap/dm/pull/704)
- Disable the meaningless auto-resume tasks triggered by the `unsupported collation` error during data migration [#735](https://github.com/pingcap/dm/pull/735)
- Optimize some logs [#660](https://github.com/pingcap/dm/pull/660) [#724](https://github.com/pingcap/dm/pull/724) [#738](https://github.com/pingcap/dm/pull/738)


## [1.0.5] 2020-04-27

### Improvements

- Improve the incremental replication speed when the `UNIQUE KEY` column has the `NULL` value
- Add retry for the `Write conflict` (9007 and 8005) error returned by TiDB

### Bug fixes

- Fix the issue that the `Duplicate entry` error might occur during the full data import
- Fix the issue that the replication task cannot be stopped or paused when the full data import is completed and the upstream has no written data  
- Fix the issue the monitoring metrics still display data after the replication task is stopped

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Improve the incremental replication speed when the `UNIQUE KEY` column has the `NULL` value [#588](https://github.com/pingcap/dm/pull/588) [#597](https://github.com/pingcap/dm/pull/597)
- Add retry for the `Write conflict` (9007 and 8005) error returned by TiDB [#632](https://github.com/pingcap/dm/pull/632)
- Fix the issue that the `Duplicate entry` error might occur during the full data import [#554](https://github.com/pingcap/dm/pull/554)
- Fix the issue that the replication task cannot be stopped or paused when the full data import is completed and the upstream has no written data [#622](https://github.com/pingcap/dm/pull/622)
- Fix the issue the monitoring metrics still display data after the replication task is stopped [#616](https://github.com/pingcap/dm/pull/616)
- Fix the issue that the `Column count doesn't match value count` error might be returned during the sharding DDL replication [#624](https://github.com/pingcap/dm/pull/624)
- Fix the issue that some metrics such as `data file size` are incorrectly displayed when the paused task of full data import is resumed [#570](https://github.com/pingcap/dm/pull/570)
- Add and fix multiple monitoring metrics [#590](https://github.com/pingcap/dm/pull/590) [#594](https://github.com/pingcap/dm/pull/594)

## [1.0.4] 2020-03-13

### Improvements

- Add English UI for DM-portal
- Add the `--more` parameter in the `query-status` command to show complete replication status information

### Bug fixes

- Fix the issue that `resume-task` might fail to resume the replication task which is interrupted by the abnormal connection to the downstream TiDB server
- Fix the issue that the online DDL operation cannot be properly replicated after a failed replication task is restarted because the online DDL meta information has been cleared after the DDL operation failure
- Fix the issue that `query-error` might cause the DM-worker to panic after `start-task` goes into error
- Fix the issue that the relay log file and `relay.meta` cannot be correctly recovered when restarting an abnormally stopped DM-worker process before `relay.meta` is successfully written

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Add English UI for DM-portal [#480](https://github.com/pingcap/dm/pull/480)
- Add the `--more` parameter in the `query-status` command to show complete replication status information [#533](https://github.com/pingcap/dm/pull/533)
- Fix the issue that `resume-task` might fail to resume the replication task which is interrupted by the abnormal connection to the downstream TiDB server [#436](https://github.com/pingcap/dm/pull/436)
- Fix the issue that the online DDL operation cannot be properly replicated after a failed replication task is restarted because the online DDL meta information is cleared after the DDL operation failure [#465](https://github.com/pingcap/dm/pull/465)
- Fix the issue that `query-error` might cause the DM-worker to panic after `start-task` goes into error [#519](https://github.com/pingcap/dm/pull/519)
- Fix the issue that the relay log file and `relay.meta` cannot be correctly recovered when restarting an abnormally stopped DM-worker process before `relay.meta` is successfully written [#534](https://github.com/pingcap/dm/pull/534)
- Fix the issue that the `value out of range` error might be reported when getting `server-id` from the upstream [#538](https://github.com/pingcap/dm/pull/538)
- Fix the issue that when Prometheus is not configured DM-Ansible prints the wrong error message that DM-master is not configured [#438](https://github.com/pingcap/dm/pull/438)

## [1.0.3] 2019-12-13

### Improvements

- Add the command mode in dmctl
- Support replicating the `ALTER DATABASE` DDL statement
- Optimize the error message output

### Bug fixes

- Fix the panic-causing data race issue occurred when the full import unit pauses or exits
- Fix the issue that `stop-task` and `pause-task` might not take effect when retrying SQL operations to the downstream

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Add the command mode in dmctl [#364](https://github.com/pingcap/dm/pull/364)
- Optimize the error message output [#351](https://github.com/pingcap/dm/pull/351)
- Optimize the output of the `query-status` command [#357](https://github.com/pingcap/dm/pull/357)
- Optimize the privilege check for different task modes [#374](https://github.com/pingcap/dm/pull/374)
- Support checking the duplicate quotaed route-rules or filter-rules in task config[#385](https://github.com/pingcap/dm/pull/385)
- Support replicating the `ALTER DATABASE` DDL statement [#389](https://github.com/pingcap/dm/pull/389)
- Optimize the retry mechanism for anomalies [#391](https://github.com/pingcap/dm/pull/391)
- Fix the panic issue caused by the data race when the import unit pauses or exits  [#353](https://github.com/pingcap/dm/pull/353)
- Fix the issue that `stop-task` and `pause-task` might not take effect when retrying SQL operations to the downstream [#400](https://github.com/pingcap/dm/pull/400)
- Upgrade golang to v1.13 and upgrade the version of other dependencies [#362](https://github.com/pingcap/dm/pull/362)
- Filter the error that the context is canceled when a SQL statement is being executed [#382](https://github.com/pingcap/dm/pull/382)
- Fix the issue that the error occurred when performing a rolling update to DM monitor using DM-ansible causes the update to fail [#408](https://github.com/pingcap/dm/pull/408)

## [1.0.2] 2019-10-30

### Improvements

- Generate some config items for DM-worker automatically
- Generate some config items for replication task automatically
- Simplify the output of `query-status` without arguments
- Manage DB connections directly for downstream

### Bug fixes

- Fix some panic when starting up or executing SQL statements
- Fix abnormal sharding DDL replication on DDL execution timeout
- Fix starting task failure caused by the checking timeout or any inaccessible DM-worker
- Fix SQL execution retry for some error

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Generate random `server-id` for DM-worker config automatically [#337](https://github.com/pingcap/dm/pull/337)
- Generate `flavor` for DM-worker config automatically [#328](https://github.com/pingcap/dm/pull/328)
- Generate `relay-binlog-name` and `relay-binlog-gtid` for DM-worker config automatically [#318](https://github.com/pingcap/dm/pull/318)
- Generate table name list for dumping in task config from black & white table lists automatically [#326](https://github.com/pingcap/dm/pull/326)
- Add concurrency items (`mydumper-thread`, `loader-thread` and `syncer-thread`) for task config [#314](https://github.com/pingcap/dm/pull/314)
- Simplify the output of `query-status` without arguments [#340](https://github.com/pingcap/dm/pull/340)
- Fix abnormal sharding DDL replication on DDL execution timeout [#338](https://github.com/pingcap/dm/pull/338)
- Fix potential DM-worker panic when restoring subtask from local meta [#311](https://github.com/pingcap/dm/pull/311)
- Fix DM-worker panic when committing a DML transaction failed [#313](https://github.com/pingcap/dm/pull/313)
- Fix DM-worker or DM-master panic when the listening port is being used [#301](https://github.com/pingcap/dm/pull/301)
- Fix retry for error code 1105 [#321](https://github.com/pingcap/dm/pull/321), [#332](https://github.com/pingcap/dm/pull/332)
- Fix retry for `Duplicate entry` and `Data too long for column` [#313](https://github.com/pingcap/dm/pull/313)
- Fix task check timeout when having large amounts of tables in upstream [#327](https://github.com/pingcap/dm/pull/327)
- Fix starting task failure when any DM-worker is not accessible [#319](https://github.com/pingcap/dm/pull/319)
- Fix potential DM-worker startup failure in GTID mode after being recovered from corrupt relay log [#339](https://github.com/pingcap/dm/pull/339)
- Fix in-memory TPS count for sync unit [#294](https://github.com/pingcap/dm/pull/294)
- Manage DB connections directly for downstream [#325](https://github.com/pingcap/dm/pull/325)
- Improve error system by refining error information passed between components [#320](https://github.com/pingcap/dm/pull/320)

## [1.0.1] 2019-09-10

#### Bug fixes

- Fix a bug that may cause database connection re-establish too frequent [#280](https://github.com/pingcap/dm/pull/280)
- Fix a potential panic bug when we query-status during subtask unit transforming between different units [#274](https://github.com/pingcap/dm/pull/274)

## [1.0.0] 2019-09-06

### v1.0.0 What's New

#### Improvements

- Add auto recovery framework
- Make task retryable when encounters a database driver error
- Make task retryable when encounters network issue
- Improve the DDL compatibility in DM

#### Bug fixes

- Fix the bug that has a risk of data loss when the upstream database connection is abnormal 

#### Action required

- When upgrading from a previous version, please note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Retry for upstream bad connection [#265](https://github.com/pingcap/dm/pull/265)
- Added some retryable errors in underlying database implementation [#256](https://github.com/pingcap/dm/pull/256)
- Added task auto recovery framework [#246](https://github.com/pingcap/dm/pull/246)
- Unify DB operation into one pkg for load and binlog replication [#236](https://github.com/pingcap/dm/pull/236)
- Fixed a bug in task handler that may cause dm-worker panic [#225](https://github.com/pingcap/dm/pull/225)
- Refactor DM error system [#216](https://github.com/pingcap/dm/pull/216)
- Add strictly config verification in dm-master, dm-worker and task [#212](https://github.com/pingcap/dm/pull/212)
- Limit DM error and log message length [#257](https://github.com/pingcap/dm/pull/257)
- Support case insensitive binlog event filter in binlog replication [#188](https://github.com/pingcap/dm/pull/188)
- Refactor DM log with pingcap/log [#195](https://github.com/pingcap/dm/pull/195)
- Use `INSERT` instead of `REPLACE` under non safe-mode [#199](https://github.com/pingcap/dm/pull/199)


## [1.0.0-rc.1] 2019-07-01

- Remove the restriction of "the next shard DDL statement cannot be executed unless the current shard DDL operation is completely finished in shard merge scene for binlog replication" [#177](https://github.com/pingcap/dm/pull/177)
- Support retry task on the `invalid connection` error for binlog replication [#66](https://github.com/pingcap/dm/pull/66)
- Support generating `-schema-create.sql` files automatically for full migration [#186](https://github.com/pingcap/dm/pull/186)
- Use [TiDB SQL Parser](https://github.com/pingcap/parser) to parse and restore DDL statements in binlog query events, and remove the `use db` statement when replicating to downstream [#54](https://github.com/pingcap/dm/pull/54)
- Support migrating tables with generated column for full migration and binlog replication [#42](https://github.com/pingcap/dm/pull/42) [#60](https://github.com/pingcap/dm/pull/60)
- Support appending the task name as a suffix to the dumped data directory (`dir` in the task configuration file) for full migration [#100](https://github.com/pingcap/dm/pull/100)
- Support resuming tasks automatically after the DM-worker process restarted [#88](https://github.com/pingcap/dm/pull/88) [#116](https://github.com/pingcap/dm/pull/116)
- Support skipping pre-check items when starting tasks [#65](https://github.com/pingcap/dm/pull/65)
- Support case-insensitive in binlog event type of Binlog Event Filter rules [#188](https://github.com/pingcap/dm/pull/188)
- Optimize relay log mechanism to improve compatibility with upstream MySQL/MariaDB [#92](https://github.com/pingcap/dm/pull/92) [#108](https://github.com/pingcap/dm/pull/108) [#117](https://github.com/pingcap/dm/pull/117) [#140](https://github.com/pingcap/dm/pull/140) [#171](https://github.com/pingcap/dm/pull/171)
- Optimize RPC framework to limit the concurrency between DM-master and DM-worker [#157](https://github.com/pingcap/dm/pull/157)
- Redirect the log of mydumper into the log of DM-worker [#93](https://github.com/pingcap/dm/pull/93)
- Support using `--print-sample-config` command flag to show the sample config of DM-worker and DM-master [#28](https://github.com/pingcap/dm/pull/28)
- Support showing the status of sharding DDL replication in the Grafana dashboard and optimize the dashboard [#96](https://github.com/pingcap/dm/pull/96) [#101](https://github.com/pingcap/dm/pull/101) [#120](https://github.com/pingcap/dm/pull/120)
- Support setting the `max_allowed_packet` parameter for the database connection to the upstream and downstream [#99](https://github.com/pingcap/dm/pull/99) 
- Support using `unsafe_cleanup` in DM Ansible to clean components of DM cluster [#128](https://github.com/pingcap/dm/pull/128)
- Fix flushing checkpoint wrongly when existing multiple sharding groups [#124](https://github.com/pingcap/dm/pull/124)
- Fix the wrong progress status when loading dumped files [#89](https://github.com/pingcap/dm/pull/89)
- Greatly improve the test to ensure correctness
- Fix many other bugs that don't affect correctness

## [1.0.0-alpha] 2019-01-18

- Support the full data migration and the incremental data migration from MySQL/MariaDB into TiDB
- Support running multi independent synchronization tasks concurrently on one DM-worker instance
- Support synchronizing a certain table of the upstream MySQL/MariaDB instance to the specified table in TiDB by table routing
- Support only synchronizing or filtering all operations of some databases or some tables by black and white lists
- Support synchronizing or filtering some of the binlog events by binlog event filter
- Support modifying auto-increment primary key fields to resolve the conflicts for shard tables by column mapping
- Support merging the original shard instances and tables into TiDB but with some restrictions
- Support synchronizing data definition changed by online DDL tools (including pt-osc and gh-ost)
- Support handling synchronization trouble caused by DDL which is not supported by TiDB
