drop database if exists `incremental_mode`;
create database `incremental_mode`;
use `incremental_mode`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
