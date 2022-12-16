set @@foreign_key_checks=1;
drop database if exists `foreign_key`;
create database `foreign_key`;
use `foreign_key`;

-- Check foreign key on delete/update cascade.
create table t1 (id int key);
create table t2 (id int key, constraint fk_1 foreign key (id) references t1 (id) on delete cascade on update cascade);
begin;
insert into t1 values (1),(2),(3),(4),(5);
insert into t2 values (1),(2),(3),(4),(5);
delete from t1 where id in (1,2);
update t1 set id=id+10 where id =3;
commit;
set @@foreign_key_checks=0;
delete from t1 where id=4;
set @@foreign_key_checks=1;

-- Check foreign key on delete/update set null.
create table t3 (id int key);
-- Manual create index for column b, since if the downstream TiDB doesn't support foreign key, then the downstream TiDB
-- won't auto-create index for column b, then the upstream and downstream table structures are inconsistent.
create table t4 (id int key, b int, index idx(b), constraint fk_2 foreign key (b) references t3 (id) on delete set null on update set null);
begin;
insert into t3 values (1),(2),(3),(4),(5);
insert into t4 values (1, 1),(2, 2),(3, 3),(4, 4),(5, 5);
delete from t3 where id in (1,2);
update t3 set id=id+10 where id =3;
commit;

-- Check foreign key on delete/update restrict.
create table t5 (id int key);
create table t6 (id int key, constraint fk_3 foreign key (id) references t5 (id) on delete restrict on update restrict);
insert into t5 values (1),(2),(3),(4),(5);
insert into t6 values (1),(2),(3),(4),(5);
set @@foreign_key_checks=0;
delete from t5 where id in (1,2);
update t5 set id=id+10 where id =3;
delete from t5 where id=4;
set @@foreign_key_checks=1;

-- Check foreign key on ddl drop table.
create table t7 (id int key);
create table t8 (id int key, constraint fk_4 foreign key (id) references t5 (id) on delete restrict on update restrict);
drop table t7, t8;

-- Test for cascade delete.
create table t9 (id int key, name varchar(10), leader int,  index(leader), constraint fk_5 foreign key (leader) references t9(id) ON DELETE CASCADE);
insert into t9 values (1, 'boss', null), (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1);
insert into t9 values (100, 'l2_a1', 10), (101, 'l2_a2', 10), (102, 'l2_a3', 10);
insert into t9 values (110, 'l2_b1', 11), (111, 'l2_b2', 11), (112, 'l2_b3', 11);
insert into t9 values (120, 'l2_c1', 12), (121, 'l2_c2', 12), (122, 'l2_c3', 12);
insert into t9 values (1000,'l3_a1', 100);
delete from t9 where id=1;

-- Test ddl add foreign key.
create table t10 (id int key, b int, index(b));
create table t11 (id int key, b int, index(b));
insert into t10 values (1,1),(2,2),(3,3);
insert into t11 values (1,1),(2,2),(3,3);
alter table t11 add constraint fk_6 foreign key (b) references t10(id) on delete cascade on update cascade;
delete from t10 where id=1;
update t10 set id=id+10 where id=2;

create table finish_mark (id int PRIMARY KEY);

