# Enhanced pre-check Design

## Background

Before the DM’s task starts, we should check some items to avoid start-task failures. You can see the details in our [document](https://docs.pingcap.com/tidb-data-migration/stable/precheck#disable-checking-items). In order to allow some users to use DM normally under certain circumstances, we can also set ignore-check-items in task config to ignore some items that you don’t want to check. Now, we find some shortcomings in regard to check-items.

### Bad user habits

We allow users to ignore all check-items, in which case the user's privilege is too large to perform unexpected operations. 

### Too much time overhead

If we have a large number of tables in source, we will take too much time in checking table schema, sharding table consistency and sharding table auto increment ID.

### Inadequate check

* Now we check auto increment ID by `column-mapping` which is deprecated. If we don't set the `column-mapping` and don't ignore the **auto_increment_ID**, the pre-check will report an error. PS: **auto_increment_ID** is only checked when **schema_of_shard_tables** is true.
* Dump privilege only checks RELOAD and SELECT. However, Dumpling supports different [consistency configurations](https://docs.pingcap.com/tidb/stable/dumpling-overview#adjust-dumplings-data-consistency-options), which need more privileges.
* If online-ddl is set by true and a DDL is in online-ddl stage, DM will have a problem in all mode. Specifically, when ghost table has been created but not been renamed, DM will report an error when the ghost table is renamed during the incremental phase. You can learn more about online-ddl [here](https://docs.pingcap.com/tidb-data-migration/stable/feature-online-ddl).
* For schema_of_shard_tables, whether it's a pessimistic task or an optimistic task, we all simply check consistency of schema by comparing all sharding tables’ structures. For optimistic mode, we can do better.
* Version checker checks that MySQLVersion >= 5.6.0 and MariadbVersion >= 10.1.2 before. However, we find more and more incompatibility problems in MySQLVersion >= 8.0.0 and Mariadb. In view of supporting MySQL 8.0 and Mariadb is experimental yet, checker will report a warning for them.

## Proposal

### Speed ​​up check

1. Support concurrent check
    - table_schema
    - schema_of_shard_tables
    - auto_increment_ID
2. We can adjust the concurrency by table numbers.

### Optimize some check

1. Auto_increment_ID only be checked in sharding mode.
    - If table exists auto increment ID, report a warning to user and tell them the method that can resolve the PK/UK conflict;
        1. If you set PK to AUTO INCREMENT, you must make sure that the primary key in sharding tables is not duplicated;
        2. If sharding tables have duplicated PK, you can refer to [document](https://docs.pingcap.com/tidb-data-migration/stable/shard-merge-best-practices#handle-conflicts-of-auto-increment-primary-key).
    - And if they have finished the resolving, such as manually created the table and removed AUTO INCREMENT, we should not report the warning.
2. Dump_privilege will check different privileges according to different [consistency](https://docs.pingcap.com/tidb/stable/dumpling-overview#adjust-dumplings-data-consistency-options) on source.
    - For all consistency, we will check
        - SELECT (only INFORMATION_SCHEMA's tables and dump tables)
    - For flush consistency:
        - RELOAD (global)
    - For lock consistency:
        - LOCK TABLES (only dump tables)
3. Add OnlineDDLChecker to check if a DDL of tables in allow list exists in online-ddl stage when DM task is all mode and online-ddl is true.
4. Enhance schema_of_shard_tables. 
    - If a task has passed the pre-checking when starting and exited, DM should keep the consistency during the task running. So we **don't check it** when restart the task.
    - If there does not exist checkpoints:
        - For all/full mode (pessimistic task): we keep **the original check**;
        - For all/full mode (optimistic task): we check whether the shard tables schema meets **the definition of [Optimistic Schema Compatibility](20191209_optimistic_ddl.md)**. If that meets, we can create tables by the compatible schema in the dump stage.
        - For incremental mode: **not check** the sharding tables’ schema, because the table schema obtained from show create table is not the schema at the point of binlog.
5. Version checker will report a warning in the following cases:
    - MySQL < 5.6.0
    - MySQL >= 8.0.0
    - Mariadb

### Restrict user usage
1. Remove all `ignore_check_items` settings from the [document](https://docs.pingcap.com/tidb-data-migration/stable/precheck#disable-checking-items). If the following items are detected to be set in the configuration, a warning will be reported.
2. If task is full/all mode, the following items will be forced to check (correspondingly, it will not be check in increment mode):
    - dump_privilege
    - schema_of_shard_tables(only for sharding mode)
3. If task is increment/all mode, the following items will be forced to check (correspondingly, it will not be check in full mode):
    - replication_privilege
    - server_id
    - binlog_enable
    - binlog_format
    - binlog_row_image
    - online_ddl(new added)
    - binlog_do_db(new added)
4. The following items will always be forced to check:
    - version
    - table_schema
    - auto_increment_ID(only for sharding mode)
5. Make the fail state more gentle, which is from `StateFailure` to `StateWarning`.
    - VersionChecker(same as version)
    - AutoIncrementKeyChecker(same as auto_increment_ID)
    - PK/UKChecker

### Move checker from [tidb-tools](https://github.com/pingcap/tidb-tools/tree/master/pkg/check) to DM

After this change, checker is deeply coupled to DM, both with dump privilege checking and optimistic pessimistic coordination. And checker is only used by DM (TiCDC and TiDB all don’t use it). So removing checkers from tidb-tools to DM is more convenient for development work。

In detail, we do not take the initiative to submit pr to the tidb-tools repository. Instead, we will replace the checker in tidb-tools step by step during the development of this feature.

So at last we will have two checker components in DM and tidb-tools. But DM will completely get rid of tidb-tools checker's ​​dependence or wrap our own checker layer on top of it.
