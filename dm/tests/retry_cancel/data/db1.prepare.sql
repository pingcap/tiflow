DROP DATABASE IF EXISTS `retry_cancel`;
CREATE DATABASE `retry_cancel`;
USE `retry_cancel`;
CREATE TABLE t1 (id BIGINT PRIMARY KEY, uid BIGINT) DEFAULT CHARSET=utf8mb4;
INSERT INTO t1 (id, uid) VALUES (1, 101), (2, 102);
INSERT INTO t1 (id, uid) VALUES (3, 103), (4, 104);
