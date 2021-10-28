use dm_syncer;
insert into t1 (id, name) values (3, 'Eddard Stark');
update t1 set name = 'Arya Stark' where id = 1;
update t1 set name = 'Catelyn Stark' where name = 'catelyn';

-- test multi column index with generated column
alter table t1 add column info json;
alter table t1 add column gen_id int as (info->"$.id");
alter table t1 add index multi_col(`id`, `gen_id`);
insert into t1 (id, name, info) values (4, 'gentest', '{"id": 123}');
insert into t1 (id, name, info) values (5, 'gentest', '{"id": 124}');
update t1 set info = '{"id": 120}' where id = 1;
update t1 set info = '{"id": 121}' where id = 2;
update t1 set info = '{"id": 122}' where id = 3;

-- test genColumnCache is reset after ddl
alter table t1 add column info2 varchar(40);
insert into t1 (id, name, info) values (6, 'gentest', '{"id": 125, "test cache": false}');
alter table t1 add unique key gen_idx(`gen_id`);
update t1 set name = 'gentestxx' where gen_id = 123;

insert into t1 (id, name, info) values (7, 'gentest', '{"id": 126}');
update t1 set name = 'gentestxxxxxx' where gen_id = 124;
-- delete with unique key
delete from t1 where gen_id > 124;
