drop database if exists `expr_filter`;
create database `expr_filter`;
use `expr_filter`;

create table t2 (id int primary key,
    should_skip int,
    c int,
    gen int as (id + 1)
);