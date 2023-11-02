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

create table t2 (
    a int primary key,
    b int,
    c int
);

insert into t2 values (1, 2, 3);
insert into t2 values (2, 3, 4);
insert into t2 values (3, 4, 5);

create table t3 (
    a int primary key,
    b int,
    c int
);

insert into t3 values (1, 2, 3);
insert into t3 values (2, 3, 4);
insert into t3 values (3, 4, 5);

drop database if exists `test1`;
create database `test1`;
use `test1`;

create table t1 (
    column0 int primary key,
    column1 int,
    column2 int
);

insert into t1 values (1, 2, 3);
insert into t1 values (2, 3, 4);
insert into t1 values (3, 4, 5);

create table finishmark(id int primary key);
