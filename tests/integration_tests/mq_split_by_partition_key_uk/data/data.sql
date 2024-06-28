drop database if exists test;
create database test;
use test;

CREATE TABLE t (a INT PRIMARY KEY NONCLUSTERED, b INT);

-- b = 222, partition = 2
INSERT INTO t VALUES (1, 222);
UPDATE t SET a = 2 WHERE a = 1;

-- b = 333333, partition = 1
INSERT INTO t VALUES (1, 333333);
UPDATE t SET a=3 WHERE a=1;

CREATE TABLE test.finish_mark (a int primary key);