use sharding1;
delete from t3 where name = 'Santa Sofía de la Piedad';
insert into t2 (uid, name, info) values (40001, 'Amaranta', '{"age": 0}'); # DML to trigger fetch schema from downstream before DDL
alter table t2 add column age int;
update t2 set uid = uid + 10000;
alter table t3 add column age int;
update t3 set age = 1;
alter table t2 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
update t3 set age = age + 10;
alter table t3 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
alter table t3 add column id_gen int as (uid + 1);
alter table t2 add column id_gen int as (uid + 1);
alter table t2 add unique (id_gen);
alter table t3 add unique (id_gen);
update t2 set age = age + 10;
alter table t2 add key multi_col_idx(uid, id_gen);
alter table t3 add key multi_col_idx(uid, id_gen);
update t3 set age = age + 10;

-- test ZERO_DATE
alter table t2 add column create_by DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00';
insert into t2 (uid, name, info, create_by) values (5, 'HaHa', '{"age": 300}', now());
insert into t2 (uid, name, info, create_by) values (6, 'HiHi', '{"age": 400}', '0000-00-00 00:00:01');
alter table t3 add column create_by DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00';
insert into t3 (uid, name, info, create_by) values (7, 'HaHa', '{"age": 300}', now());
insert into t3 (uid, name, info, create_by) values (8, 'HiHi', '{"age": 400}', '0000-00-00 00:00:01')
