drop database if exists `generate_column`;
create database `generate_column`;
use `generate_column`;

create table t (a int, b int as (a + 1) stored primary key);
insert into t(a) values (1),(2), (3);
update t set a = 10 where a = 1;

create table t1 (a int, b int as (a + 1) virtual not null, unique index idx(b));
insert into t1 (a) values (1),(2), (3);
update t1 set a = 10 where a = 1;

