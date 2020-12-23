drop database if exists `new_ci_collation_test`;
create database `new_ci_collation_test`;
use `new_ci_collation_test`;

CREATE TABLE t1 (
    a varchar(20) charset utf8mb4 collate utf8mb4_general_ci primary key,
    b int default 10
);

CREATE TABLE t2 (
    a varchar(10) charset utf8 collate utf8_general_ci, primary key(a),
    b int default 10
);

CREATE TABLE t3 (
    id int primary key auto_increment,
    a varchar(20) charset utf8mb4 collate utf8mb4_general_ci,
    b int default 10
);

CREATE TABLE t4 (
    a int primary key,
    b varchar(10) charset utf8mb4 collate utf8mb4_general_ci,
    c varchar(10) charset utf8 collate utf8_general_ci,
    d varchar(10) not null,
    unique key d(d)
);

CREATE TABLE t5 (
    a varchar(10) charset utf8mb4 collate utf8mb4_bin, primary key(a),
    b int default 10
);


insert into t1 (a) values ('A'),(' A'),('A\t'),('b'),('bA'),('bac'),('ab');
insert into t1 (a) values ('😉');
insert into t2 (a) values ('A'),(' A'),('A\t'),('b'),('bA'),('bac'),('ab');
insert into t3 (a) values ('A'),('A '),('A   '),(' A'),('A\t'),('A\t ');
insert into t3 (a) values ('a'),('a '),('a   '),(' a'),('a\t'),('a\t ');
insert into t3 (a) values ('B'),('B '),('B   '),(' B'),('B\t'),('B\t ');
insert into t3 (a) values ('b'),('b '),('b   '),(' b'),('b\t'),('b\t ');
insert into t4 values (1,'A','A','1'),(2,'a\t','a\t','2'),(3,'ab','ab','3'),(4,'abc','abc','4');
insert into t5 (a) values ('😉');
insert into t5 (a) values ('a'),('A'),(' a'),(' A'),('a\t'),('ab'),('Ab');
update t1 set b = b + 1;
update t2 set b = 13;
update t3 set b = 11 where a > 'A';
drop index `primary` on t4;
update t5 set b = 12;
