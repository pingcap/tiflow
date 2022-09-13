drop database if exists `expr_filter`;
create database `expr_filter`;
use `expr_filter`;

create table t2 (id int primary key,
    should_skip int,
    c int,
    gen int as (id + 1)
);

create table t6 (id int, name varchar(20), primary key(`id`)) character set latin1;
insert into t6 (id, name) values (0, 'Müller');
