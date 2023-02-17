drop database if exists `s3_dumpling_lightning2`;
create database `s3_dumpling_lightning2`;
use `s3_dumpling_lightning2`;
create table t2 (id int, name varchar(20), primary key(`id`));
insert into t2 (id, name) values (21, '21'), (22, '12');
insert into t2 (id, name) values (23, '23'), (24, '25');
insert into t2 (id, name) values (25, '25');

