drop database if exists `new_relay`;
create database `new_relay`;
use `new_relay`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
