drop database if exists `all_mode`;
create database `all_mode`;
use `all_mode`;

create table t_extract
(
    id int primary key auto_increment,
    j  json,
    a  int as (j ->> '$.a'),
    b  char(10) as (j ->> '$.b')
);

insert into t_extract(j)values ('{"a":1, "b": {"a":1}}');
insert into t_extract(j)values ('{"a":1, "b": [1,2,3]}');
insert into t_extract(j)values ('[1,2]');
insert into t_extract(j)values ('true');
insert into t_extract(j)values ('false');
insert into t_extract(j)values (null);
insert into t_extract(j)values ('"string"');
insert into t_extract(j)values ('1');
insert into t_extract(j)values ('3.14');
insert into t_extract(j)values ('{"a":1, "b": {"a":1}, "c": "string"}');

create table t_funcs
(
    id int primary key auto_increment,
    j  json,
    j1 json as (json_extract(j, '$.a')),
    j2 json as (json_extract(j1, '$.b')),
    c char(20) as (j2->>'$[0].a')
);

insert into t_funcs(j) values ('{"a": {"b": {"a":1}}}');
insert into t_funcs(j) values ('{"a":[{"a": {"b": {"a":1}}}, 1, 2, "string", [1,2,3]]}');
insert into t_funcs(j) values ('[1,2]');
insert into t_funcs(j) values ('true');
insert into t_funcs(j) values ('false');
insert into t_funcs(j) values (null);
insert into t_funcs(j) values ('"string"');
insert into t_funcs(j) values ('1');
insert into t_funcs(j) values ('3.14');
insert into t_funcs(j) values ('{"a":1, "b": {"a":1}, "c": "string"}');
