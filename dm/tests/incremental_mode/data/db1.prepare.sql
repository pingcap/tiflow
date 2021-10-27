drop database if exists `incremental_mode`;
create database `incremental_mode`;
use `incremental_mode`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
