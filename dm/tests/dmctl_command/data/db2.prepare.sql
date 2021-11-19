drop database if exists `dmctl_command`;
create database `dmctl_command`;
use `dmctl_command`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
