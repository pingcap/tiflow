drop database if exists `clustered_index_test`;
create database `clustered_index_test`;
use `clustered_index_test`;

CREATE TABLE t0 (
	id VARCHAR(255),
	data INT,
	PRIMARY KEY(id)
);

INSERT INTO t0 VALUES ('1', 1);
INSERT INTO t0 VALUES ('2', 2);
INSERT INTO t0 VALUES ('3', 3);
INSERT INTO t0 VALUES ('4', 4);
INSERT INTO t0 VALUES ('5', 5);

DELETE FROM t0 WHERE id = '3';
DELETE FROM t0 WHERE data = 5;
UPDATE t0 SET id = '10' WHERE data = 1;
UPDATE t0 SET data = 555 WHERE id = '10';

CREATE TABLE t1 (
    id VARCHAR(255),
    a INT,
    b CHAR(10),
    PRIMARY KEY(id, b),
    UNIQUE KEY(b),
    KEY(a)
);

INSERT INTO t1 VALUES ('111', 111, '111');
INSERT INTO t1 VALUES ('222', 222, '222');
INSERT INTO t1 VALUES ('333', 333, '333');
INSERT INTO t1 VALUES ('444', 444, '444');
INSERT INTO t1 VALUES ('555', 555, '555');
UPDATE t1 SET id = '10' WHERE id = '111';
DELETE FROM t1 WHERE a = 222;

CREATE TABLE t2 (
    id VARCHAR(255),
    a INT,
    b DECIMAL(5,2),
    PRIMARY KEY(id, a),
    KEY(id, a),
    UNIQUE KEY(id, a)
);

INSERT INTO t2 VALUES ('aaaa', 1111, 11.0);
INSERT INTO t2 VALUES ('bbbb', 1111, 12.0);
INSERT INTO t2 VALUES ('cccc', 1111, 13.0);
INSERT INTO t2 VALUES ('dddd', 1111, 14.0);
INSERT INTO t2 VALUES ('eeee', 1111, 15.0);


create table t_bit(a bit primary key, b int);
INSERT INTO t_bit VALUES(1,2);
INSERT INTO t_bit VALUES(0,3);

create table t_bool(a bool primary key, b int);
INSERT INTO t_bool VALUES(true,2);
INSERT INTO t_bool VALUES(false,3);

create table t_tinyint(a tinyint primary key, b int);
INSERT INTO t_tinyint VALUES(6,2);
INSERT INTO t_tinyint VALUES(8,3);

create table t_smallint(a smallint primary key, b int);
INSERT INTO t_smallint VALUES(432,2);
INSERT INTO t_smallint VALUES(125,3);

create table t_mediumint(a mediumint primary key, b int);
INSERT INTO t_mediumint VALUES(8567,2);
INSERT INTO t_mediumint VALUES(12341,3);

create table t_int(a int primary key, b int);
INSERT INTO t_int VALUES(123563,2);
INSERT INTO t_int VALUES(6784356,3);

create table t_date(a date primary key, b int);
INSERT INTO t_date VALUES ('2020-02-20', 1);
INSERT INTO t_date VALUES ('2020-02-21', 2);
INSERT INTO t_date VALUES ('2020-02-22', 3);
UPDATE t_date SET a = '2020-02-23' WHERE b = 3;
DELETE FROM t_date WHERE b = 2;


create table t_time(a time primary key, b int);

INSERT INTO t_time VALUES ('11:22:33', 1);
INSERT INTO t_time VALUES ('11:33:22', 2);
INSERT INTO t_time VALUES ('11:43:11', 3);
UPDATE t_time SET a = '11:44:55' WHERE b = 3;
DELETE FROM t_time WHERE b = 2;

create table t_datetime(a datetime primary key, b int);
INSERT INTO t_datetime VALUES ('2020-02-20 11:22:33', 1);
INSERT INTO t_datetime VALUES ('2020-02-21 11:33:22', 2);
INSERT INTO t_datetime VALUES ('2020-02-22 11:43:11', 3);
UPDATE t_datetime SET a = '2020-02-23 11:44:55' WHERE b = 3;
DELETE FROM t_datetime WHERE b = 2;

create table t_timestamp(a timestamp primary key, b int);
INSERT INTO t_timestamp VALUES ('2020-02-20 11:22:33', 1);
INSERT INTO t_timestamp VALUES ('2020-02-21 11:33:22', 2);
INSERT INTO t_timestamp VALUES ('2020-02-22 11:43:11', 3);
UPDATE t_timestamp SET a = '2020-02-23 11:44:55' WHERE b = 3;
DELETE FROM t_timestamp WHERE b = 2;

create table t_year(a year primary key, b int);
INSERT INTO t_year VALUES ('2020', 1);
INSERT INTO t_year VALUES ('2021', 2);
INSERT INTO t_year VALUES ('2022', 3);
UPDATE t_year SET a = '2023' WHERE b = 3;
DELETE FROM t_year WHERE b = 2;


create table t_char(a char(20) primary key, b int);
INSERT INTO t_char VALUES ('abcc', 1);
INSERT INTO t_char VALUES ('sdff', 2);
UPDATE t_char SET a = 'ppooii' WHERE b = 2;
DELETE FROM t_char WHERE b = 1;

create table t_varcher(a varchar(255) primary key, b int);
INSERT INTO t_varcher VALUES ('abcc', 1);
INSERT INTO t_varcher VALUES ('sdff', 2);
UPDATE t_varcher SET a = 'ppooii' WHERE b = 2;
DELETE FROM t_varcher WHERE b = 1;

create table t_text (a text, b int, primary key(a(5)));
INSERT INTO t_text VALUES ('abcc', 1);
INSERT INTO t_text VALUES ('sdff', 2);
UPDATE t_text SET a = 'ppooii' WHERE b = 2;
DELETE FROM t_text WHERE b = 1;

create table t_binary(a binary(20) primary key, b int);
INSERT INTO t_binary VALUES (x'89504E470D0A1A0A',1),(x'89504E470D0A1A0B',2),(x'89504E470D0A1A0C',3);
update t_binary set a = x'89504E470D0A1A0D' where b = 3;
delete from t_binary where b = 2;

create table t_blob(a blob, b int, primary key (a(20)));
INSERT INTO t_blob VALUES (x'89504E470D0A1A0A',1),(x'89504E470D0A1A0B',2),(x'89504E470D0A1A0C',3);
update t_blob set a = x'89504E470D0A1A0D' where b = 3;
delete from t_binary where b = 2;

create table t_enum(e enum('a', 'b', 'c') primary key, b int);
INSERT INTO t_enum VALUES ('a',1),('b',2),('c',3);
delete from t_enum where b = 2;
update t_enum set e = 'b' where b = 3;

create table t_set(s set('a', 'b', 'c') primary key, b int);
INSERT INTO t_set VALUES ('a',1),('b,c',2),('a,c',3);
delete from t_set where b = 2;
update t_set set s = 'b' where b = 3;


create table t8(a int, b varchar(255) as (concat(a, "test")) stored, primary key(b));
INSERT INTO t8(a) VALUES (2020);
INSERT INTO t8(a) VALUES (2021);
INSERT INTO t8(a) VALUES (2022);
UPDATE t8 SET a = 2023 WHERE a = 2022;
DELETE FROM t8 WHERE a = 2021;


create table t9(a int, b varchar(255), c int, primary key(a ,b));
insert into t9 values(1, "aaa", 1),(2, "bbb", 2),(3, "ccc", 3);
update t9 set b='ddd' where c = 3;
delete from t9 where c = 2;

create table t10(a int, b int, c int, primary key(a, b));
insert into t10 values(1, 1, 1),(2, 2, 2),(3, 3, 3);
update t10 set b = 4 where a = 3;
delete from t10 where a = 2;

create table t11(a int, b float, c int, primary key(a,b));
insert into t11 values(1, 1.1, 1),(2, 2.2, 2),(3, 3.3, 3);
update t11 set b = 4.4 where c = 3;
delete from t11 where b = 2;

create table t12(name char(255) primary key, b int, c int, index idx(name), unique index uidx(name));
insert into t12 values("aaaa", 1, 1), ("bbb", 2, 2), ("ccc", 3, 3);
update t12 set name = 'ddd' where c = 3;
delete from t12 where c = 2;
