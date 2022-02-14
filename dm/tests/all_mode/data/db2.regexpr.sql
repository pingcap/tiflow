-- test regular expression router
drop database if exists `test4s_2022`;
create database `test4s_2022`;
use `test4s_2022`;
create table testtable_donot_delete (
    a int,
    b int,
    primary key(a)
);
insert into testtable_donot_delete values
(1, 1),
(2, 2),
(3, 3),
(4, 4);