# Proposal: Binlog replication(syncer) unit support plugin

- Author(s):    [wangxiang](https://github.com/WangXiangUSTC)
- Last updated: 2020-04-13

## Abstract

This proposal introduces why we need to support plugin in binlog replication(syncer) unit, and how to implements it.

## Background

Some users have customization requirements like below, but it is not suitable to implement in DM. We can support plugin in DM, and then user can implement their requirements by plugin.

### Replication incompatible DDL in TiDB

DM is a tool used to replication data from MySQL to TiDB, we know that TiDB is compatible with MySQL in most case, but some DDL is not supported in TiDB now. For example: TiDB can't reduce column's length, if you execute these SQLs in MySQL:

```SQL
CREATE DATABASE test;
CREATE TABLE test.t1(id int primary key, name varchar(100));
ALTER TABLE test.t1 MODIFY COLUMN name varchar(50);
```

And then DM will replication these SQLs to TiDB and get error `Error 1105: unsupported modify column length 50 is less than origin 100`

DM and TiDB can't handle these SQLs now, users need to execute compatible DDL in TiDB and then skip this DDL by [binlog-event-filter](https://pingcap.com/docs/tidb-data-migration/stable/feature-overview/#binlog-event-filter) in DM. It is not convenient for users, and cannot be automated.

In fact, the incompatible DDL `ALTER TABLE test.t1 MODIFY COLUMN name varchar(50)` can be translated to:

```SQL
ALTER TABLE test.t1 ADD COLUMN name_tmp varchar(50) AFTER id;
REPLACE INTO test.t1(id, name_tmp) SELECT id, name AS name_tmp FROM test.t1;
ALTER TABLE test.t1 DROP COLUMN name;
ALTER TABLE test.t1 CHANGE COLUMN name_tmp name varchar(50);
```

Maybe we can execute them automated when meet `unsupported modify column` error.

### Double write

DM only support replication data to TiDB, but some users
want to send binlog to other platform while replication to TiDB.

For example, user expect to send DDL binlog to Kafka after DDL is replication to TiDB, and then read binlog from Kafka for notifing business change.

## Implementation

### Interface

For handle DDL which is not supported in TiDB or used for double write, we need to design at least three interface in plugin.

#### Init

We can do some initial job in the `Init` interface, for example, create connection to the downstream platform(like Kafka) for double write.

#### HandleDDLJobResult

This interface used to handle the DDL job's result in binlog replication unit(syncer)

- When the ddl job execute failed, judge the error type by error code or error message, then do something to resolve it.
- When the ddl job execute success, then send it to other platform.

#### HandleDMLJobResult

This interface used to handle the DML job's result in binlog replication unit(syncer)

- When the dml job execute success, then send it to other platform.

### Hook

`Init` can be execute when [initial](https://github.com/pingcap/dm/blob/9023c789964fde0f5134e0c49435db557e21fdf7/syncer/syncer.go#L257) binlog replication unit.

`HandleDDLJobResult` handle the result of [handleQueryEvent](https://github.com/pingcap/dm/blob/9023c789964fde0f5134e0c49435db557e21fdf7/syncer/syncer.go#L1279) and then do something.

`HandleDMLJobResult` handle the result of [handleRowsEvent](https://github.com/pingcap/dm/blob/9023c789964fde0f5134e0c49435db557e21fdf7/syncer/syncer.go#L1274) and then do something.

## How to use

1. User implements these interface designed in plugin
2. Build the go file in plugin mode, and generate a `.so` file
3. Set plugin in task's config file
4. Binlog replication load the plugin
