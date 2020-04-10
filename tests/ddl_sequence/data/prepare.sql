drop database if exists `ddl_sequence`;
create database `ddl_sequence`;
use `ddl_sequence`;

CREATE TABLE many_cols1 (
	id INT AUTO_INCREMENT PRIMARY KEY,
	val INT DEFAULT 0,
	col0 INT NOT NULL,
)
ALTER TABLE many_cols1 DROP COLUMN col0;
INSERT INTO many_cols1 (val) VALUES (1);
DROP TABLE many_cols1;

CREATE TABLE many_cols1 (
	id INT AUTO_INCREMENT PRIMARY KEY,
	val INT DEFAULT 0,
	col0 INT NOT NULL,
)
ALTER TABLE many_cols1 DROP COLUMN col0;
INSERT INTO many_cols1 (val) VALUES (1);
DROP TABLE many_cols1;

CREATE TABLE many_cols1 (
	id INT AUTO_INCREMENT PRIMARY KEY,
	val INT DEFAULT 0,
	col0 INT NOT NULL,
)
ALTER TABLE many_cols1 DROP COLUMN col0;
INSERT INTO many_cols1 (val) VALUES (1);
DROP TABLE many_cols1;

CREATE TABLE many_cols1 (
	id INT AUTO_INCREMENT PRIMARY KEY,
	val INT DEFAULT 0,
	col0 INT NOT NULL,
)
ALTER TABLE many_cols1 DROP COLUMN col0;
INSERT INTO many_cols1 (val) VALUES (1);
DROP TABLE many_cols1;

CREATE TABLE many_cols1 (
	id INT AUTO_INCREMENT PRIMARY KEY,
	val INT DEFAULT 0,
	col0 INT NOT NULL,
)
ALTER TABLE many_cols1 DROP COLUMN col0;
INSERT INTO many_cols1 (val) VALUES (1);
DROP TABLE many_cols1;