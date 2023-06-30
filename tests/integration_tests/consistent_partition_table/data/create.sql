drop database if exists `partition_table`;
set @@global.tidb_enable_exchange_partition=on;
create database `partition_table`;
use `partition_table`;

create table t (a int, primary key (a)) partition by hash(a) partitions 5;
create table t1 (a int primary key) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
alter table t1 add partition (partition p3 values less than (30), partition p4 values less than (40));

create table t2 (a int primary key);
create database `partition_table2`;
create table partition_table2.t2 (a int primary key);