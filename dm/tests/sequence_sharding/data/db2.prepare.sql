drop database if exists `sharding_seq`;
create database `sharding_seq`;
use `sharding_seq`;
create table t2 (id bigint auto_increment,uid int,name varchar(20),primary key (`id`),unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
create table t3 (id bigint auto_increment,uid int,name varchar(20),primary key (`id`),unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
create table t4 (id bigint auto_increment,uid int,name varchar(20),primary key (`id`),unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
insert into t2 (uid,name) values (300001,'io'),(300002,'xOKvsDsofmAzEF');
insert into t3 (uid,name) values (400001,'eXcRSo'),(400002,'QOP'),(400003,'DUotcCayM');
insert into t4 (uid,name) values (500001,'`id` = 15'),(500002,'942032497'),(500003,'UrhcHUbwsDMZrvJxM');
