drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
create table dm_syncer_1(id int, name varchar(20), primary key(`id`));
insert into dm_syncer_1 values (1, 'arya'), (2, 'catelyn');

drop database if exists `dm_syncer_do_db`;
create database `dm_syncer_do_db`;
use `dm_syncer_do_db`;
create table dm_syncer_do_db_1(id int, name varchar(20), primary key(`id`));
insert into dm_syncer_do_db_1(id, name) values (1, 'Howie'), (2, 'howie');
create table dm_syncer_do_db_2(id int, name varchar(20), primary key(`id`));
insert into dm_syncer_do_db_2(id, name) values (1, 'Howie'), (2, 'howie');
create table dm_syncer_do_db_3(id int, name varchar(20), primary key(`id`));
insert into dm_syncer_do_db_3(id, name) values (1, 'Howie'), (2, 'howie');

drop database if exists `dm_syncer_ignore_db`;
create database `dm_syncer_ignore_db`;
use `dm_syncer_ignore_db`;
create table dm_syncer_ignore_db_1(id int, name varchar(20), primary key(`id`));
insert into dm_syncer_ignore_db_1(id, name) values (1, 'Howie'), (2, 'howie');
create table dm_syncer_ignore_db_2(id int, name varchar(20), primary key(`id`));
insert into dm_syncer_ignore_db_2(id, name) values (1, 'Howie'), (2, 'howie');
