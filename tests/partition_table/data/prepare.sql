drop database if exists `partition_table`;
create database `partition_table`;
use `partition_table`;

create table t (a int, primary key (a)) partition by hash(a) partitions 5;
insert into t values (1),(2),(3),(4),(5),(6);
insert into t values (7),(8),(9);

create table t1 (a int primary key) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p3 VALUES LESS THAN (21));
insert into t1 values (1),(2),(3),(4),(5),(6);
insert into t1 values (7),(8),(9);
insert into t1 values (11),(12),(20);
