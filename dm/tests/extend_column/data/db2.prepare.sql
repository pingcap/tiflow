drop database if exists `extend_column2`;
create database `extend_column2`;
use `extend_column2`;
create table t2 (c1 int, c2 int, c3 int, primary key(c1));
insert into t2 values(1, 1, 1);
insert into t2 values(2, 2, 2);
