drop database if exists `ha_master_test`;
create database `ha_master_test`;
use `ha_master_test`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
