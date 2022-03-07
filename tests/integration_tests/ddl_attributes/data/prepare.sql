DROP DATABASE IF EXISTS `ddl_attributes`;
CREATE DATABASE `ddl_attributes`;
USE `ddl_attributes`;

CREATE TABLE attributes_t1 (id INT PRIMARY KEY, name VARCHAR(50));
ALTER TABLE attributes_t1 ATTRIBUTES='merge_option=deny';
INSERT INTO attributes_t1 (id, name) VALUES (1, "test1");

CREATE TABLE attributes_t2 (id INT PRIMARY KEY, name VARCHAR(50)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10000), PARTITION p1 VALUES LESS THAN (MAXVALUE));
ALTER TABLE attributes_t2 ATTRIBUTES='merge_option=deny';
ALTER TABLE attributes_t2 PARTITION p0 ATTRIBUTES='merge_option=allow';
INSERT INTO attributes_t2 (id, name) VALUES (2, "test2");

DROP TABLE attributes_t1;
RECOVER TABLE attributes_t1;
TRUNCATE TABLE attributes_t1;
FLASHBACK TABLE attributes_t1 TO attributes_t1_back;
RENAME TABLE attributes_t1_back TO attributes_t1_new;
ALTER TABLE attributes_t2 DROP PARTITION p0;
ALTER TABLE attributes_t2 TRUNCATE PARTITION p1;

CREATE TABLE finish_mark(a INT PRIMARY KEY);
