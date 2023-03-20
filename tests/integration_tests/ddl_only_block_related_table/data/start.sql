drop database if exists `ddl_only_block_related_table`;
create database `ddl_only_block_related_table`;
use `ddl_only_block_related_table`;

create table t1 (id int primary key);
create table t2 (id int primary key);
create table ddl_not_done (id int primary key);

insert into t1 values (1);
insert into t2 values (1);
insert into ddl_not_done values (1);

create table finish_mark (id int primary key);
