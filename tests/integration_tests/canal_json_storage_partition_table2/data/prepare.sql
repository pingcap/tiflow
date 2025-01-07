drop database if exists `partition_table`;
create database `partition_table`;
use `partition_table`;

create table t1 (a int primary key) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
-- partition p0
insert into t1 values (1),(2),(3);
-- partition p1
insert into t1 values (7),(8),(9);
-- partition p2
insert into t1 values (11),(12),(20);

