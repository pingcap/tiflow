drop database if exists `load_task1`;
create database `load_task1`;
use `load_task1`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');

drop database if exists `load_task2`;
create database `load_task2`;
use `load_task2`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');

drop database if exists `load_task4`;
create database `load_task4`;
use `load_task4`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
