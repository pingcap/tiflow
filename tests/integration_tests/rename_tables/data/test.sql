drop database if exists `rename_tables_test`;
create database `rename_tables_test`;
use `rename_tables_test`;

create table t1 (
 value64  bigint unsigned  not null,
 primary key(value64)
);
insert into t1 values(17156792991891826145);
insert into t1 values(91891826145);
delete from t1 where value64=17156792991891826145;
update t1 set value64=17156792991891826;
update t1 set value64=56792991891826;

rename table t1 to t1_1;

create table t2 (
 value64  bigint unsigned  not null,
 primary key(value64)
);
insert into t2 values(17156792991891826145);
insert into t2 values(91891826145);
delete from t2 where value64=91891826145;
update t2 set value64=17156792991891826;
update t2 set value64=56792991891826;

rename table t2 to t2_2;

create table t1 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

create table t2 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

insert into t1 values(17156792991891826145, 1);
insert into t1 values( 9223372036854775807, 2);
insert into t2 values(17156792991891826145, 3);
insert into t2 values( 9223372036854775807, 4);

rename table t1 to t1_7, t2 to t2_7;

insert into t1_7 values(91891826145, 5);
insert into t1_7 values(685477580, 6);
insert into t2_7 values(1715679991826145, 7);
insert into t2_7 values(2036854775807, 8);

create table finish_mark(id int primary key);
