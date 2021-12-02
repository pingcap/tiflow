# Enhanced pre-check Design

## Background

Before the DM’s task starts, we should check some items to avoid start-task failures. You can see the details in our [document](https://docs.pingcap.com/zh/tidb-data-migration/stable/precheck#%E5%85%B3%E9%97%AD%E6%A3%80%E6%9F%A5%E9%A1%B9). In order to allow some users to use DM normally under certain circumstances, we can also set ignore-check-items in task config to ignore some items that you don’t want to check. Now, we find some shortcomings in regard to check-items.

### Bad user habits

We allow users to ignore all check-items, in which case the user's authority is too large to perform unexpected operations. 

### Too much time overhead

If we have a large number of tables in source, we will take too much time in checking table schema, sharding table consistency and sharding table auto increment key.

### Inadequate check

* If downstream creates tables manually and the new downstream’s auto increment ID is not the same as the upstream, we shouldn’t check **auto_increment_ID** for errors. Users should be responsible for what they set.
* Dump privilege only checks RELOAD and SELECT. However, Dumpling supports different [consistency configurations](https://docs.pingcap.com/zh/tidb/stable/dumpling-overview#%E8%B0%83%E6%95%B4-dumpling-%E7%9A%84%E6%95%B0%E6%8D%AE%E4%B8%80%E8%87%B4%E6%80%A7%E9%80%89%E9%A1%B9), which need more privilege.
* If online-ddl is set by true and a DDL is in online-ddl stage, DM will have a problem in all mode. Specifically, ghost table has been created, is executing the DDL, but is not renamed yet. In this case, DM will report an error when the ghost table is renamed after the dump phase. You can learn more about online-ddl [here](https://docs.pingcap.com/zh/tidb-data-migration/stable/feature-online-ddl).
* For schema_of_shard_tables, whatever pessimistic task and optimistic task, we all check it by comparing all sharding tables’ structures for consistency simply. For optimistic mode, we can do better.

## Proposal

### Restrict user usage
1. Remove the following settings from the [document](https://docs.pingcap.com/zh/tidb-data-migration/stable/precheck#%E5%85%B3%E9%97%AD%E6%A3%80%E6%9F%A5%E9%A1%B9). If the following items are detected to be set in the configuration, a warning will be reported.
    - all
    - dump_privilege
    - replication_privilege
    - server_id
    - binlog_enable
    - binlog_format
    - binlog_row_image
2. If task is full/all mode, the following items will be forced to check (correspondingly, it will not be check in increment mode):
    - dump_privilege
3. If task is increment/all mode, the following items will be forced to check (correspondingly, it will not be check in full mode):
    - replication_privilege
    - server_id
    - binlog_enable
    - binlog_format
    - binlog_row_image
4. Other checkers are the same as before. If you want to ignore them, you should set them in ignore_check_items.
    - table_schema
    - auto_increment_ID
    - schema_of_shard_tables

### Speed ​​up check

1. Support concurrent check
    - table_schema
    - schema_of_shard_tables
    - auto_increment_ID
2. Use mydumper.threads as **source_connection_concurrency**, which should update in our document.

#### How to speed up?

Since every checker is concurrent, we can split tables to **source_connection_concurrency** part, and create a checker for every part. 

### Optimize some check

1. We needn’t check **auto_increment_ID** in following situation:
    - If downstream creates tables manually and the new downstream’s auto increment ID is not the same as the upstream;
    - If the column of auto increment ID in upstream does not has an unique constraint in downstream.
2. Dump_privilege will check different privileges according to different [consistency](https://docs.pingcap.com/zh/tidb/stable/dumpling-overview#%E8%B0%83%E6%95%B4-dumpling-%E7%9A%84%E6%95%B0%E6%8D%AE%E4%B8%80%E8%87%B4%E6%80%A7%E9%80%89%E9%A1%B9) and downstream on source.
    - For all consistency, we will check
        - REPLICATION CLIENT (global)
        - SELECT (only dump table)
    - For flush consistency：
        - RELOAD (global)
    - For flush/lock consistency：
        - LOCK TABLES (only dump table)
    - For TiDB downstream：
        - PROCESS (global)
3. Add OnlineDDLChecker to check if a DDL of tables in allow list exists in online-ddl stage when DM task is all mode and online-ddl is true. It will be forced to check in all mode and not checked in increment mode.
4. Enhance schema_of_shard_tables. 
    - At first, if a machine exits the DM’s checkpoint and then DM start/resume task, we think the checkpoint guarantees consistency. So we don't check it.
    - If not exit checkpoint:
        - For all/full mode (pessimistic task): we keep the original check;
        - For all/full mode (optimistic task): we check whether the shard tables schema meets the definition of [Optimistic Schema Compatibility](20191209_optimistic_ddl.md). If that meets, we can create tables by the compatible schema in the dump stage.
        - For incremental mode: not check the sharding tables’ schema, because the table schema obtained from show create table is not the schema at the point of binlog.
5. Make the fail state more gentle, which is from `StateFailure` to `StateWarning`.
    - checkAutoIncrementKey
    - checkPK/UK

### Remove checker from tidb-tools to DM

After this change, checker is deeply coupled to DM, both with dump privilege and optimistic pessimistic coordination. And checker is only used by DM (TiCDC and TiDB all don't use it). So removing checker from tidb-tools to DM is more convenient for development work。
