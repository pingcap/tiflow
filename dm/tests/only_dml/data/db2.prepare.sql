drop database if exists `only_dml`;
reset master;
create database `only_dml`;
use `only_dml`;
create table t2 (id int, primary key (`id`));
insert into t2 values (2);
