drop database if exists `gbk2`;
create database `gbk2` character set gbk;
use `gbk2`;
create table t1 (id int, name varchar(20), primary key(`id`)) character set gbk;
insert into t1 (id, name) values (1, '你好Aa'), (2, '你好aA');
