-- test regular expression router
drop database if exists `test4s_2022`;
create database `test4s_2022`;
use `test4s_2022`;
create table testtablelbclbc (
    a int,
    b int,
    primary key(a)
);
insert into testtablelbclbc values
(1, 1),
(2, 2),
(3, 3),
(4, 4);