DROP DATABASE IF EXISTS `partial_index`;
CREATE DATABASE `partial_index`;
USE `partial_index`;

CREATE TABLE t1 (
  id INT PRIMARY KEY,
  col INT,
  col2 INT,
  KEY idx_partial_create(col) WHERE col2 > 100
);
INSERT INTO t1 (id, col, col2) VALUES (1, 10, 50), (2, 20, 120);

CREATE TABLE t2 (
  id INT PRIMARY KEY,
  col INT,
  col2 INT
);
INSERT INTO t2 (id, col, col2) VALUES (1, 10, 50), (2, 20, 120);
CREATE INDEX idx_partial_create_index ON t2 (col) WHERE col2 > 100;

CREATE TABLE t3 (
  id INT PRIMARY KEY,
  col INT,
  col2 INT
);
INSERT INTO t3 (id, col, col2) VALUES (1, 10, 50), (2, 20, 120);
ALTER TABLE t3 ADD INDEX idx_partial_alter(col) WHERE col2 > 100;

CREATE TABLE finish_mark(a INT PRIMARY KEY);
