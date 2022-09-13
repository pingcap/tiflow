drop database if exists `all_mode`;
create database `all_mode`;
use `all_mode`;

create table t_extract
(
    id int primary key auto_increment,
    j  json,
    a  int as (j ->> '$.a'),
    b  char(10) as (j ->> '$.b'),
    j1 json as (json_search(j, 'one', '1')),
    j2 json as (json_array(j ->> '$.a', j ->> '$.b')),
    j3 json as (json_object('a', j ->> '$.a', 'b', j ->> '$.b')),
    j4 json as (json_merge_preserve(b, '{"k": "v"}')),
    j5 json as (json_merge_patch(b, '{"k": "v"}')),
    j7 json as (json_set(b, '$.k', 'v')),
    j8 json as (json_insert(b, '$.k', 'v')),
    j9 json as (json_replace(b, '$.k', 'v')),
    j10 json as (json_remove(b, '$.k')),
    j11 int as (json_contains(b, '{"k": "v"}')),
    j12 int as (json_contains_path(b, 'one', '$.k')),
    j13 json as (json_array_append(b, '$[0]', 'v')),
    j14 json as (json_array_insert(j3, '$[0]', 'v'))
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
