CREATE SCHEMA test_basic COLLATE utf8mb4_general_ci;
USE test_basic;

CREATE TABLE t1 (
  id int PRIMARY KEY,
  name varchar(255) NOT NULL
);

INSERT INTO t1 VALUES (1, 'test 1');
INSERT INTO t1 VALUES (2, 'test 2'), (3, 'test 3');
UPDATE t1 SET name='test 2 updated' WHERE id=2;
DELETE FROM t1 WHERE id=3;
