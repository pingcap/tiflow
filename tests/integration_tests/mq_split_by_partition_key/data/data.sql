drop database if exists test;
create database test;
use test;

CREATE TABLE t (a INT PRIMARY KEY NONCLUSTERED, b INT);

INSERT INTO t VALUES (1, 2);
UPDATE t SET a = 2 WHERE a = 1;

INSERT INTO t VALUES (1, 3);
UPDATE t SET a="3" WHERE a="1";

CREATE TABLE test.finish_mark (a int primary key);