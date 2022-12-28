drop database if exists `sharding_seq`;
create database `sharding_seq`;
use `sharding_seq`;
create table t1 (id bigint auto_increment,uid int,name varchar(20),primary key (`id`),unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin auto_increment=100000;
create table t2 (id bigint auto_increment,uid int,name varchar(20),primary key (`id`),unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin auto_increment=200000;
insert into t1 (uid,name) values (100001,'igvApUx'),(100002,'qUyrcOBkwDK');
insert into t2 (uid,name) values (200001,'EycletJHetWHMfH'),(200002,'ytkIaCOwXnWmy'),(200003,'MWQeWw""''rNmtGxzGp');
