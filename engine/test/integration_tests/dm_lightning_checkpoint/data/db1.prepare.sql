drop user if exists 'dm_full';
flush privileges;
create user 'dm_full'@'%' identified by '123456';
grant all privileges on *.* to 'dm_full'@'%';
revoke replication slave, replication client, super on *.* from 'dm_full'@'%';
revoke create temporary tables, lock tables, create routine, alter routine, event, create tablespace, file, shutdown, execute, process, index on *.* from 'dm_full'@'%'; # privileges not supported by TiDB
flush privileges;

drop database if exists dm_full_empty_db;
create database dm_full_empty_db;

drop database if exists dm_full_empty_table;
create database dm_full_empty_table;
create table dm_full_empty_table.t1 (id int primary key);

drop database if exists dm_full_route_schema;
create database dm_full_route_schema;
create table dm_full_route_schema.t1 (id int primary key);
insert into dm_full_route_schema.t1 values (1);

drop database if exists `dm_full_special/ch"ar`;
create database `dm_full_special/ch"ar`;
create table `dm_full_special/ch"ar`.`t/b"1` (id int primary key, name varchar(255));
insert into `dm_full_special/ch"ar`.`t/b"1` values (1, 'a');

drop database if exists `dm_full`;
create database `dm_full`;
use `dm_full`;
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
