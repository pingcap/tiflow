drop database if exists `compatibility`;
create database `compatibility`;
use `compatibility`;
create table t1 (id int, name varchar(20), primary key (`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
