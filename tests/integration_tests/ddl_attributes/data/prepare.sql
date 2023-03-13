DROP DATABASE IF EXISTS `ddl_attributes`;
CREATE DATABASE `ddl_attributes`;
USE `ddl_attributes`;

CREATE TABLE attributes_t1 (id INT PRIMARY KEY, name VARCHAR(50));
ALTER TABLE attributes_t1 ATTRIBUTES='merge_option=deny';
INSERT INTO attributes_t1 (id, name) VALUES (1, "test1");
INSERT INTO attributes_t1 (id) VALUES (2);

CREATE TABLE attributes_t2 (id INT PRIMARY KEY, name VARCHAR(50)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10000), PARTITION p1 VALUES LESS THAN (MAXVALUE));
ALTER TABLE attributes_t2 ATTRIBUTES='merge_option=deny';
ALTER TABLE attributes_t2 PARTITION p0 ATTRIBUTES='merge_option=allow';
INSERT INTO attributes_t2 (id, name) VALUES (2, "test2");
INSERT INTO attributes_t2 (id) VALUES (3);

DROP TABLE attributes_t1;
RECOVER TABLE attributes_t1;
TRUNCATE TABLE attributes_t1;
FLASHBACK TABLE attributes_t1 TO attributes_t1_back;
RENAME TABLE attributes_t1_back TO attributes_t1_new;
ALTER TABLE attributes_t2 DROP PARTITION p0;
ALTER TABLE attributes_t2 TRUNCATE PARTITION p1;

DROP PLACEMENT POLICY IF EXISTS placement1;
CREATE PLACEMENT POLICY placement1 followers=2;
CREATE TABLE placement_t1 (id BIGINT NOT NULL PRIMARY KEY auto_increment, b varchar(255)) PLACEMENT POLICY=placement1;
CREATE TABLE `placement_t2` (id BIGINT NOT NULL PRIMARY KEY auto_increment) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`placement1` */;

DROP TABLE IF EXISTS ttl_t1, ttl_t2, ttl_t3, ttl_t4;
CREATE TABLE ttl_t1(id BIGINT NOT NULL PRIMARY KEY auto_increment, t datetime) TTL=`t` + INTERVAL 1 DAY;
CREATE TABLE ttl_t2(id BIGINT NOT NULL PRIMARY KEY auto_increment, t datetime);
CREATE TABLE ttl_t3(id BIGINT NOT NULL PRIMARY KEY auto_increment, t datetime) TTL=`t` + INTERVAL 1 DAY TTL_ENABLE='OFF';
CREATE TABLE ttl_t4(id BIGINT NOT NULL PRIMARY KEY auto_increment, t datetime) TTL=`t` + INTERVAL 1 DAY;
CREATE TABLE ttl_t5(id BIGINT NOT NULL PRIMARY KEY auto_increment, t datetime) TTL=`t` + INTERVAL 1 DAY;
ALTER TABLE ttl_t2 TTL=`t` + INTERVAL 1 DAY;
ALTER TABLE ttl_t3 TTL_ENABLE='ON';
ALTER TABLE ttl_t4 REMOVE TTL;
ALTER TABLE ttl_t5 TTL_JOB_INTERVAL='7h';

CREATE TABLE finish_mark(a INT PRIMARY KEY);
