drop database if exists `multi_tables_ddl_test`;
create database `multi_tables_ddl_test`;
use `multi_tables_ddl_test`;

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

create table t3 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

create table t4 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error changefeed*/
create table t5 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error1 changefeed*/
create table t6 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error1 changefeed*/
create table t7 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error1 changefeed*/
create table t8 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error2 changefeed*/
create table t10 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error2 changefeed*/
create table t11 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

insert into t1 values(17156792991891826145, 1);
insert into t1 values( 9223372036854775807, 2);
insert into t2 values(17156792991891826145, 3);
insert into t2 values( 9223372036854775807, 4);

insert into t5 values(17156792991891826145, 1);
insert into t6 values( 9223372036854775807, 2);
insert into t7 values(17156792991891826145, 3);
insert into t8 values( 9223372036854775807, 4);


rename table t1 to t1_7, t2 to t2_7;

insert into t1_7 values(91891826145, 5);
insert into t1_7 values(685477580, 6);
insert into t2_7 values(1715679991826145, 7);
insert into t2_7 values(2036854775807, 8);

insert into t3 select * from t1_7;
insert into t4 select * from t2_7;
drop table t3, t4;



/* cf_err1, filter.rules = ["multi_tables_ddl_test.t5", "multi_tables_ddl_test.t6", "multi_tables_ddl_test.t7, "multi_tables_ddl_test.t8"] */
/* replicate successful, they are all in `filter.rule` */
rename table t5 to t55, t6 to t66;
/* discard by cdc totally, they are all not in `filter.rule` */
rename table t55 to t555, t66 to t666;
/* replicate successful, since t8 in `filter.rule` */
rename table t8 to t88;
/* discard, t88 and t888 both not in `filter.rule` */
rename table t88 to t888;


/* cf_err2, filter.rules = ["multi_tables_ddl_test.t9", "multi_tables_ddl_test.t10"] */
/* replicate successful, since t10 in `filter.rule` */
rename table t10 to t9;
/* replicate successful, since t9 in `filter.rule` */
rename table t9 to t13;
/* discard, t13 and t14 both not in `filter.rule` */
rename table t13 to t14;
/* error, t11 not match `filter.rule` and t9 match `filter.rule` */
rename table t11 to t9;
create table finish_mark(id int primary key);
