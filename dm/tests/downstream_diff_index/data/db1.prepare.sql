drop database if exists `downstream_diff_index1`;
create database `downstream_diff_index1`;
use `downstream_diff_index1`;
create table t1 (c1 int, c2 int, c3 varchar(10), primary key(c1));
insert into t1 values(1, 1, '1');
insert into t1 values(2, 2, '2');
insert into t1 values(3, 3, '3');
create table varchar20000 (c int primary key, c2 varchar(20000)) charset=utf8;
