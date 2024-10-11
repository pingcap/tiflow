drop database if exists `partition_table`;
drop database if exists `partition_table2`;
create database `partition_table`;
use `partition_table`;
set tidb_enable_global_index = ON;

create table t (a int, b varchar(255), primary key (a), unique key (b) global) partition by hash(a) partitions 5;
insert into t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6);
insert into t values (7,7),(8,8),(9,9);
alter table t truncate partition p3;
update t set a=a+10, b = "12" where a=2;


create table t1 (a int primary key, b bigint, unique key (b) global) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
insert into t1 (a) values (1),(2),(3),(4),(5),(6);
insert into t1 (a) values (7),(8),(9);
insert into t1 (a) values (11),(12),(20);
update t1 set b = a;
alter table t1 add partition (partition p3 values less than (30), partition p4 values less than (40));
insert into t1 values (25,25),(29,29),(35,35); /*these values in p3,p4*/
alter table t1 truncate partition p0;
alter table t1 drop partition p1;
insert into t1 values (7,7),(8,8),(9,9);
update t1 set a=a+10, b = 19 where a=9;

/* Remove partitioning + add partitioning back again */
alter table t remove partitioning;
insert into t values (20,20),(21,21),(22,22),(23,23),(24,24),(25,25);
alter table t partition by hash (a) partitions 5 update indexes (b global);
insert into t values (30,30),(31,31),(32,32),(33,33),(34,34),(35,35);

/* exchange partition case 1: source table and target table in same database */
/* exchange partition does not support global index */
create table t1b like t1;
alter table t1b drop index b;
insert into t1b select * from t1;
create table t2 (a int primary key, b bigint);
ALTER TABLE t1b EXCHANGE PARTITION p3 WITH TABLE t2;
insert into t2 values (100,100),(101,101),(102,102),(103,103),(104,104),(105,105); /*these values will be replicated to in downstream t2*/
insert into t1b values (25,25),(29,29); /*these values will be replicated to in downstream t1b.p3*/

/* exchange partition ccase 2: source table and target table in different database */
create database `partition_table2`;
create table partition_table2.t2 (a int primary key, b bigint);
ALTER TABLE t1b EXCHANGE PARTITION p3 WITH TABLE partition_table2.t2;
insert into partition_table2.t2 values (1002,1002),(1012,1012),(1022,1022),(1032,1032),(1042,1042),(1052,1052); /*these values will be replicated to in downstream t2*/
insert into t1b values (21,21),(28,28); /*these values will be replicated to in downstream t1.p3*/

truncate table t1;
insert into t1 select * from t1b;
ALTER TABLE t1 REORGANIZE PARTITION p0,p2 INTO (PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (21));
insert into t1 values (-1,-1),(6,6),(13,13);
update t1 set a=a-22, b = -2 where a=20;
delete from t1 where a = 5;
ALTER TABLE t1 REORGANIZE PARTITION p2,p3,p4 INTO (PARTITION p2 VALUES LESS THAN (20), PARTITION p3 VALUES LESS THAN (26), PARTITION p4 VALUES LESS THAN (35), PARTITION pMax VALUES LESS THAN (MAXVALUE));
insert into t1 values (-3,-3),(5,5),(14,14),(22,22),(30,30),(100,100);
update t1 set a=a-16, b = -4 where a=12;
delete from t1 where a = 29;

/* Change partitioning to key based and then back to range */
alter table t1 partition by key(a) partitions 7;
insert into t1 values (-2001,-2001),(2001,2001),(2002,2002),(-2002,-2002),(-2003,-2003),(2003,2003),(-2004,-2004),(2004,2004),(-2005,-2005),(2005,2005),(2006,2006),(-2006,-2006),(2007,2007),(-2007,-2007);
ALTER TABLE t1 partition by range(a) (partition p0 values less than (5), PARTITION p2 VALUES LESS THAN (20), PARTITION p3 VALUES LESS THAN (26), PARTITION p4 VALUES LESS THAN (35), PARTITION pMax VALUES LESS THAN (MAXVALUE));

create table finish_mark (a int primary key);
