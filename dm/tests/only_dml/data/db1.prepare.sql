drop database if exists `only_dml`;
reset master;
create database `only_dml`;
use `only_dml`;
create table t1 (id int, primary key(`id`));
insert into t1 values (1);
