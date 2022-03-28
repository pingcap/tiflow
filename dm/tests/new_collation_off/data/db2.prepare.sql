drop database if exists `new_collation_off`;
create database `new_collation_off`;
use `new_collation_off`;
create table t1 (
    id int PRIMARY KEY,
    name varchar(20) COLLATE utf8mb4_0900_as_cs
);

insert into t1 (id, name) values (1, 'Alice');
