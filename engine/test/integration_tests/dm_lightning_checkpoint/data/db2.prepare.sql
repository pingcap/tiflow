drop user if exists 'dm_full2';
flush privileges;
create user 'dm_full2'@'%' identified by '123456';
grant all privileges on *.* to 'dm_full2'@'%';
flush privileges;

drop database if exists `dm_full2`;
create database `dm_full2` CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin;
use `dm_full2`;
create table t1 (
    id int,
    name varchar(20),
    ts timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    primary key(`id`));
insert into t1 (id, name, ts) values (1, 'arya', now()), (2, 'catelyn', '2021-05-11 10:01:03');
insert into t1 (id, name) values (3, 'Eddard
Stark');
update t1 set name = 'Arya S\\\\tark' where id = 1;
update t1 set name = 'Catelyn S\"\n\ttark' where name = 'catelyn';

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
