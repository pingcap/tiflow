set collation_server = utf8mb4_0900_ai_ci;
drop database if exists `sync_collation`;
create database `sync_collation`;
use `sync_collation`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'Aa'), (2, 'aA');
create table t2 (id int, name varchar(20) character set utf8, primary key(`id`));
insert into t2 (id, name) values (1, 'Aa'), (2, 'aA');
