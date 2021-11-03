drop database if exists `downstream_diff_index2`;
create database `downstream_diff_index2`;
use `downstream_diff_index2`;
create table t2 (c1 int, c2 int, c3 varchar(10), primary key(c1));
insert into t2 values(1, 12, '13');
insert into t2 values(2, 22, '23');
insert into t2 values(3, 32, '33');
