drop database if exists `new_ci_collation_test`;
create database `new_ci_collation_test`;
use `new_ci_collation_test`;

CREATE TABLE t1 (
    a varchar(20) charset utf8mb4 collate utf8mb4_general_ci primary key,
    b int default 10
);

CREATE TABLE t2 (
    id int primary key auto_increment,
    a varchar(20) charset utf8mb4 collate utf8mb4_general_ci,
    b int default 10
);

CREATE TABLE t3 (
    a int primary key,
    b varchar(10) collate utf8mb4_general_ci,
    c varchar(10) not null,
    unique key c(c)
);

CREATE TABLE t4 (
    a varchar(10) charset utf8mb4 collate utf8mb4_bin, primary key(a)
);


insert into t1 (a) values ('A'),(' A'),('A\t'),('b'),('bA'),('bac'),('ab');
insert into t1 (a) values ('😉');
insert into t2 (a) values ('A'),('A '),('A   '),(' A'),('A\t'),('A\t ');
insert into t2 (a) values ('a'),('a '),('a   '),(' a'),('a\t'),('a\t ');
insert into t2 (a) values ('B'),('B '),('B   '),(' B'),('B\t'),('B\t ');
insert into t2 (a) values ('b'),('b '),('b   '),(' b'),('b\t'),('b\t ');
insert into t3 values (1,'A','1'),(2,'a\t','2'),(3,'ab','3'),(4,'abc','4');
insert into t4 values ('a'),('A'),(' a'),(' A'),('a\t'),('ab'),('Ab');
update t1 set b = b + 1;
update t2 set b = 11 where a > 'A';
drop index `primary` on t3;
