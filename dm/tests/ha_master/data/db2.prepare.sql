drop database if exists `ha_master_test`;
create database `ha_master_test`;
use `ha_master_test`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
