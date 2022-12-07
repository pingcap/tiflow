drop database if exists `expr_filter`;
create database `expr_filter`;
use `expr_filter`;

create table t2 (id int primary key,
    should_skip int,
    c int,
    gen int as (id + 1)
);

create table t6 (id int, name varchar(20), msg text, primary key(`id`)) character set latin1;
insert into t6 (id, name, msg) values (0, 'Müller', 'Müller');
CREATE TABLE t7 (a BIGINT PRIMARY KEY, r VARCHAR(10), s INT);
INSERT INTO t7 VALUES (1, 'a', 2);
