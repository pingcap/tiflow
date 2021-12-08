drop database if exists `force_replicate_table`;
create database `force_replicate_table`;
use `force_replicate_table`;

CREATE TABLE t0 (id bigint primary key, a int);
CREATE TABLE t1 (id bigint not null unique key, a int);
CREATE TABLE t2 (id bigint unique key, a int);
CREATE TABLE t3 (id bigint, a int);
CREATE TABLE t4 (id varchar(20) unique key, a int);
CREATE TABLE t5 (id varchar(20), a int);

insert into t0 (id) values (1),(2),(3),(4),(5);
insert into t1 (id) values (1),(2),(3),(4),(5);
insert into t2 (id) values (null),(null),(1),(2),(3),(4),(5);
insert into t3 (id) values (null),(null),(1),(1),(2),(3),(4);
insert into t4 (id) values (null),(null),('1'),('2'),('3'),('4');
insert into t5 (id) values (null),(null),('1'),('1'),('2'),('3'),('4');

update t0 set a = 1;
update t1 set a = 1;
update t2 set a = 1;
update t3 set a = 1;
update t4 set a = 1;
update t5 set a = 1;

alter table t0 drop primary key;
alter table t1 drop key id;

update t0 set a = 2 where id > 3;
update t1 set a = 2 where id > 2;

delete from t0 where id < 3;
delete from t1 where id < 3;
delete from t2 where id < 3;
delete from t3 where id < 3;
delete from t4 where id < '3';
delete from t5 where id < '3';

create table t6 (id int primary key, a int) PARTITION BY RANGE ( id ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
insert into t6 (id) values (1),(2),(3),(4),(5),(6);
insert into t6 (id) values (7),(8),(9);
insert into t6 (id) values (11),(12),(20);
alter table t6 add partition (partition p3 values less than (30), partition p4 values less than (40));
insert into t6 (id) values (25),(29),(35);
alter table t6 truncate partition p0;
alter table t6 drop partition p1;
insert into t6 (id) values (7),(8),(9);
update t6 set id=id+10 where id=9;
