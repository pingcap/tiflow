drop database if exists `dup_event`;
create database `dup_event`;
use `dup_event`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');