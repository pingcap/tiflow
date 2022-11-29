drop database if exists `binlog_parse`;
create database `binlog_parse` character set utf8;
use `binlog_parse`;
create table t1 (id int, created_time timestamp, primary key(`id`)) character set utf8;
create table t2 (id int, created_time timestamp(3), primary key(`id`)) character set utf8;
insert into t1 (id, created_time) values (1, '2022-01-01 00:00:01'), (2, '2022-01-02 00:00:01');
insert into t2 (id, created_time) values (1, '2022-01-01 00:00:01'), (2, '2022-01-02 00:00:01');