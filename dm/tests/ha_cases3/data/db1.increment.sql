use database-placeholder;
insert into ta (id, name) values (3, 'Eddard Stark');
update ta set name = 'Arya Stark' where id = 1;
update ta set name = 'Catelyn Stark' where name = 'catelyn';

-- test multi column index with generated column
alter table ta add column info json;
alter table ta add column gen_id int as (info->"$.id");
alter table ta add index multi_col(`id`, `gen_id`);
insert into ta (id, name, info) values (4, 'gentest', '{"id": 123}');
insert into ta (id, name, info) values (5, 'gentest', '{"id": 124}');
update ta set info = '{"id": 120}' where id = 1;
update ta set info = '{"id": 121}' where id = 2;
update ta set info = '{"id": 122}' where id = 3;

-- test genColumnCache is reset after ddl
alter table ta add column info2 varchar(40);
insert into ta (id, name, info) values (6, 'gentest', '{"id": 125, "test cache": false}');
alter table ta add unique key gen_idx(`gen_id`);
update ta set name = 'gentestxx' where gen_id = 123;

insert into ta (id, name, info) values (7, 'gentest', '{"id": 126}');
update ta set name = 'gentestxxxxxx' where gen_id = 124;
-- delete with unique key
delete from ta where gen_id > 124;

-- test alter database
-- alter database database-placeholder CHARACTER SET = utf8mb4;

-- test decimal type
alter table ta add column lat decimal(9,6) default '0.000000';
insert into ta (id, name, info, lat) values (8, 'gentest', '{"id":127}', '123.123')
