use all_mode;

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

alter table t_extract add column alt_a  int as (j ->> '$.a');
alter table t_extract add column alt_b  char(10) as (j ->> '$.b');
alter table t_extract add column alt_j1 json as (json_search(j, 'one', '1'));
alter table t_extract add column alt_j2 json as (json_array(j ->> '$.a', j ->> '$.b'));
alter table t_extract add column alt_j3 json as (json_object('a', j ->> '$.a', 'b', j ->> '$.b'));
alter table t_extract add column alt_j4 json as (json_merge_preserve(b, '{"k": "v"}'));
alter table t_extract add column alt_j5 json as (json_merge_patch(b, '{"k": "v"}'));
alter table t_extract add column alt_j7 json as (json_set(b, '$.k', 'v'));
alter table t_extract add column alt_j8 json as (json_insert(b, '$.k', 'v'));
alter table t_extract add column alt_j9 json as (json_replace(b, '$.k', 'v'));
alter table t_extract add column alt_j10 json as (json_remove(b, '$.k'));
alter table t_extract add column alt_j11 int as (json_contains(b, '{"k": "v"}'));
alter table t_extract add column alt_j12 int as (json_contains_path(b, 'one', '$.k'));
alter table t_extract add column alt_j13 json as (json_array_append(b, '$[0]', 'v'));
alter table t_extract add column alt_j14 json as (json_array_insert(j3, '$[0]', 'v'));

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
