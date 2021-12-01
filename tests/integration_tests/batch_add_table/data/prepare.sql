drop database if exists `batch_add_table`;
create database `batch_add_table`;
use `batch_add_table`;

create table a1 (id int primary key);
create table a2 (id int primary key);
create table a3 (id int primary key);
create table a4 (id int primary key);
create table a5 (id int primary key);

insert into a1 values (1);
insert into a2 values (1);
insert into a3 values (1);
insert into a4 values (1);
insert into a5 values (1);