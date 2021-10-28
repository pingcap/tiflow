drop database if exists `gtid`;
reset master;
create database `gtid`;
use `gtid`;
create table t2 (id int primary key);
insert into t2 values (1);