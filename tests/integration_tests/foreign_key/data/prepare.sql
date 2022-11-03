set @@global.tidb_enable_foreign_key=1;
set @@foreign_key_checks=1;
drop database if exists `foreign_key`;
create database `foreign_key`;
use `foreign_key`;

-- Check foreign key on delete/update cascade.
create table t1 (id int key);
create table t2 (id int key, foreign key (id) references t1 (id) on delete cascade on update cascade);
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
create table t1 (id int key);
create table t3 (id int key);
create table t4 (id int key, b int, foreign key (b) references t3 (id) on delete set null on update set null);
begin;
insert into t3 values (1),(2),(3),(4),(5);
insert into t4 values (1, 1),(2, 2),(3, 3),(4, 4),(5, 5);
delete from t3 where id in (1,2);
update t3 set id=id+10 where id =3;
commit;

-- Check foreign key on delete/update restrict.
create table t5 (id int key);
create table t6 (id int key, foreign key (id) references t5 (id) on delete restrict on update restrict);
insert into t5 values (1),(2),(3),(4),(5);
insert into t6 values (1),(2),(3),(4),(5);
set @@foreign_key_checks=0;
delete from t5 where id in (1,2);
update t5 set id=id+10 where id =3;
delete from t5 where id=4;
set @@foreign_key_checks=1;

-- Check foreign key on ddl drop table.
create table t7 (id int key);
create table t8 (id int key, foreign key (id) references t5 (id) on delete restrict on update restrict);
drop table t7, t8;

create table finish_mark (id int PRIMARY KEY);

