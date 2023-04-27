drop database if exists `load_task1`;
create database `load_task1`;
use `load_task1`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');

drop database if exists `load_task2`;
create database `load_task2`;
use `load_task2`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');

drop database if exists `load_task3`;
create database `load_task3`;
use `load_task3`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
