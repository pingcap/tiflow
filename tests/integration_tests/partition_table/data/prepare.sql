drop database if exists `partition_table`;
drop database if exists `partition_table2`;
create database `partition_table`;
use `partition_table`;

create table t (a int, primary key (a)) partition by hash(a) partitions 5;
insert into t values (1),(2),(3),(4),(5),(6);
insert into t values (7),(8),(9);
alter table t truncate partition p3;
update t set a=a+10 where a=2;


create table t1 (a int primary key) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
insert into t1 values (1),(2),(3),(4),(5),(6);
insert into t1 values (7),(8),(9);
insert into t1 values (11),(12),(20);
alter table t1 add partition (partition p3 values less than (30), partition p4 values less than (40));
insert into t1 values (25),(29),(35); /*these values in p3,p4*/
alter table t1 truncate partition p0;
alter table t1 drop partition p1;
insert into t1 values (7),(8),(9);
update t1 set a=a+10 where a=9;

/* Remove partitioning + add partitioning back again */
alter table t remove partitioning;
insert into t values (20),(21),(22),(23),(24),(25);
alter table t partition by hash (a) partitions 5;
insert into t values (30),(31),(32),(33),(34),(35);

/* exchange partition case 1: source table and target table in same database */
create table t2 (a int primary key);
ALTER TABLE t1 EXCHANGE PARTITION p3 WITH TABLE t2;
insert into t2 values (100),(101),(102),(103),(104),(105); /*these values will be replicated to in downstream t2*/
insert into t1 values (25),(29); /*these values will be replicated to in downstream t1.p3*/

/* exchange partition ccase 2: source table and target table in different database */
create database `partition_table2`;
create table partition_table2.t2 (a int primary key);
ALTER TABLE t1 EXCHANGE PARTITION p3 WITH TABLE partition_table2.t2;
insert into partition_table2.t2 values (1002),(1012),(1022),(1032),(1042),(1052); /*these values will be replicated to in downstream t2*/
insert into t1 values (21),(28); /*these values will be replicated to in downstream t1.p3*/

ALTER TABLE t1 REORGANIZE PARTITION p0,p2 INTO (PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (21));
insert into t1 values (-1),(6),(13);
update t1 set a=a-22 where a=20;
delete from t1 where a = 5;
ALTER TABLE t1 REORGANIZE PARTITION p2,p3,p4 INTO (PARTITION p2 VALUES LESS THAN (20), PARTITION p3 VALUES LESS THAN (26), PARTITION p4 VALUES LESS THAN (35), PARTITION pMax VALUES LESS THAN (MAXVALUE));
insert into t1 values (-3),(5),(14),(22),(30),(100);
update t1 set a=a-16 where a=12;
delete from t1 where a = 29;

/* Change partitioning to key based and then back to range */
alter table t1 partition by key(a) partitions 7;
insert into t1 values (-2001),(2001),(2002),(-2002),(-2003),(2003),(-2004),(2004),(-2005),(2005),(2006),(-2006),(2007),(-2007);
ALTER TABLE t1 partition by range(a) (partition p0 values less than (5), PARTITION p2 VALUES LESS THAN (20), PARTITION p3 VALUES LESS THAN (26), PARTITION p4 VALUES LESS THAN (35), PARTITION pMax VALUES LESS THAN (MAXVALUE));

create table finish_mark (a int primary key);
