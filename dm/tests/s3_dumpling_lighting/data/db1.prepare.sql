drop database if exists `s3_dumpling_lightning1`;
create database `s3_dumpling_lightning1`;
use `s3_dumpling_lightning1`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (11, '11'), (12, '12');
insert into t1 (id, name) values (13, '13'), (14, '14');
create table t11 (id int, name varchar(20), primary key(`id`));
insert into t11 (id, name) values (111, '111'), (112, '112');
insert into t11 (id, name) values (113, '113'), (114, '114');
