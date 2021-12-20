drop database if exists `gbk`;
create database `gbk` character set utf8;
use `gbk`;
create table t1 (id int, name varchar(20), primary key(`id`)) character set utf8;
insert into t1 (id, name) values (1, '你好1'), (2, '你好2');
