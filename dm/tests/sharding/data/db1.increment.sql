use sharding1;
insert into t1 (uid, name) values (10003, 'Buenos Aires');
update t1 set name = 'Gabriel José de la Concordia García Márquez' where `uid` = 10001;
update t1 set name = 'One Hundred Years of Solitude' where name = 'Cien años de soledad';
insert into t2 (uid, name, info) values (20013, 'Colonel', '{}'); # DML to trigger fetch schema from downstream before DDL
alter table t1 add column age int;
insert into t2 (uid, name, info) values (20023, 'Aureliano', '{}');
insert into t2 (uid, name, info) values (20033, 'Buendía', '{}');
alter table t2 add column age int;
insert into t2 (uid, name, age, info) values (20004, 'Colonel Aureliano Buendía', 301, '{}');
alter table t2 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
insert into t1 (uid, name, info) values (10004, 'Buenos Aires', '{"age": 10}');
insert into t2 (uid, name, info) values (20005, 'Buenos Aires', '{"age": 100}');
insert into t2 (uid, name, info) values (20006, 'Buenos Aires', '{"age": 1000}');
alter table t1 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
alter table t1 add column id_gen int as (uid + 1);
alter table t2 add column id_gen int as (uid + 1);
alter table t1 add unique (id_gen);
alter table t2 add unique (id_gen);
insert into t1 (uid, name, info) values (10005, 'Buenos Aires', '{"age": 100}');
insert into t2 (uid, name, info) values (20007, 'Buenos Aires', '{"age": 200}');
alter table t1 add key multi_col_idx(uid, id_gen);
alter table t2 add key multi_col_idx(uid, id_gen);
insert into t1 (uid, name, info) values (10006, 'Buenos Aires', '{"age": 100}');
insert into t2 (uid, name, info) values (20008, 'Buenos Aires', '{"age": 200}');

-- test ZERO_DATE
alter table t2 add column create_by DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00';
insert into t2 (uid, name, info, create_by) values (1, 'HaHa', '{"age": 300}', now());
insert into t2 (uid, name, info, create_by) values (2, 'HiHi', '{"age": 400}', '0000-00-00 00:00:01');
alter table t1 add column create_by DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00';
insert into t1 (uid, name, info, create_by) values (3, 'HaHa', '{"age": 300}', now());
insert into t1 (uid, name, info, create_by) values (4, 'HiHi', '{"age": 400}', '0000-00-00 00:00:01');
