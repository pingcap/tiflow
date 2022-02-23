-- test regular expression router
drop database if exists `test2animal`;
create database `test2animal`;
use `test2animal`;
create table tbl_animal_dogcat (
    a int,
    b int,
    primary key(a)
);
insert into tbl_animal_dogcat values
(1, 1),
(2, 2),
(3, 3);