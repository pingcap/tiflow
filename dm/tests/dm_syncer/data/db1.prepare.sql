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

use `dm_syncer_do_db`;
create table dm_syncer_do_db_4(id int, foreign_id int, primary key (id), foreign key (foreign_id) references dm_syncer_ignore_db.dm_syncer_ignore_db_1(id));
insert into dm_syncer_do_db_4(id, foreign_id) values (1, 1), (2, 2);
create table dm_syncer_do_db_5 like dm_syncer_do_db_4;
insert into dm_syncer_do_db_5(id, foreign_id) values (1, 1), (2, 2);