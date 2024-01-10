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


CREATE TABLE tt1 (
                    a varchar(20) charset utf8mb4 collate utf8mb4_0900_ai_ci primary key,
                    b int default 10
);

CREATE TABLE tt2 (
                     a varchar(20) charset utf8mb4 collate utf8mb4_0900_ai_ci,
                     b int default 10, primary key(b)
);

CREATE TABLE tt3 (
                    id int primary key auto_increment,
                    a varchar(20) charset utf8mb4 collate utf8mb4_0900_ai_ci,
                    b int default 10
);

CREATE TABLE tt4 (
                    a int primary key,
                    b varchar(10) charset utf8mb4 collate utf8mb4_0900_ai_ci,
                    c varchar(10) charset utf8mb4 collate utf8mb4_0900_ai_ci,
                    d varchar(10) not null,
                    unique key d(d)
);

insert into tt1 (a) values ('A'),(' A'),('A\t'),('b'),('bA'),('bac'),('ab'),('\U000FFFFE'),('Àoo'),('ß'),('æ'),('aeoo'), ('𝕒bc');
insert into tt1 (a) values ('😉');
insert into tt2 (a, b) values ('A', 1),(' A', 2),('A\t', 3),('b', 4),('bA', 5),('bac', 6),('ab', 7),('\U000FFFFE', 8),('À', 9),('ß', 10),('æ', 11),('ae', 12), ('𝕒bc', 13);
insert into tt2 (a, b) values ('😉', 14);
insert into tt3 (a) values ('A'),('A '),('A   '),(' A'),('A\t'),('A\t '),('\U000FFFFE'),('À'),('ß'),('æ'),('ae'), ('𝕒bc');
insert into tt3 (a) values ('a'),('a '),('a   '),(' a'),('a\t'),('a\t '),('\U000FFFFE'),('À'),('ß'),('æ'),('ae'), ('𝕒bc');
insert into tt3 (a) values ('B'),('B '),('B   '),(' B'),('B\t'),('B\t '),('\U000FFFFE'),('À'),('ß'),('æ'),('ae'), ('𝕒bc');
insert into tt3 (a) values ('b'),('b '),('b   '),(' b'),('b\t'),('b\t '),('\U000FFFFE'),('À'),('ß'),('æ'),('ae'), ('');
insert into tt4 values (1,'A','A','1'),(2,'a\t','a\t','2'),(3,'ab','ab','3'),(4,'abc','abc','4'),(5,'À','À','5'),(6,'ß','ß','6'),(7,'æ','æ','7'),(8,'ae','ae','8'),(9,'𝕒bc','𝕒bc','9');
update tt1 set b = b + 1;
update tt3 set b = 11 where a > 'A';
update tt3 set b = '14' where a > 'À';
drop index `primary` on tt4;
