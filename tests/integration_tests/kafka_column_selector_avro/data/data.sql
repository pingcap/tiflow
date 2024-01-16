drop database if exists `test`;
create database `test`;
use `test`;

create table t1 (
    a int primary key,
    b int,
    c int
);

insert into t1 values (1, 2, 3);
insert into t1 values (2, 3, 4);
insert into t1 values (3, 4, 5);

create table finishmark(id int primary key);
