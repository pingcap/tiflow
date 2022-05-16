-- This sql file is encoded in GBK by the 'iconv' command.
-- DO NOT EDIT.

DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
SET NAMES gbk;
USE test;

-- gbk dmls
CREATE TABLE cs_gbk (
	id INT,
	name varchar(128) CHARACTER SET gbk,
	country char(32) CHARACTER SET gbk,
	city varchar(64),
	description text CHARACTER SET gbk,
	image tinyblob,
	PRIMARY KEY (id)
) ENGINE = InnoDB CHARSET = utf8mb4;

INSERT INTO cs_gbk
VALUES (1, '测试', "中国", "上海", "你好,世界"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO cs_gbk
VALUES (2, '部署', "美国", "纽约", "世界,你好"
	, 0xCAC0BDE7C4E3BAC3);

UPDATE cs_gbk
SET name = '开发'
WHERE name = '测试';

DELETE FROM cs_gbk
WHERE name = '部署'
	AND country = '美国'
	AND city = '纽约'
	AND description = '世界,你好';

-- ddls
CREATE TABLE test_ddl1
(
    id INT AUTO_INCREMENT,
    c1 INT,
    PRIMARY KEY (id)
);

CREATE TABLE test_ddl2
(
    id INT AUTO_INCREMENT,
    c1 INT,
    PRIMARY KEY (id)
);

RENAME TABLE test_ddl1 TO test_ddl;

ALTER TABLE test_ddl
    ADD INDEX test_add_index (c1);

DROP INDEX test_add_index ON test_ddl;

ALTER TABLE test_ddl
    ADD COLUMN c2 INT NOT NULL;

TRUNCATE TABLE test_ddl;

DROP TABLE test_ddl2;

CREATE TABLE test_ddl2
(
    id INT AUTO_INCREMENT,
    c1 INT,
    PRIMARY KEY (id)
);

CREATE TABLE test_ddl3 (
	id INT,
	名称 varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

ALTER TABLE test_ddl3
	ADD COLUMN 城市 char(32);

ALTER TABLE test_ddl3
	MODIFY COLUMN 城市 varchar(32);

ALTER TABLE test_ddl3
	DROP COLUMN 城市;

/* this is a DDL test for table */
CREATE TABLE 表1 (
	id INT,
	name varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

RENAME TABLE 表1 TO 表2;

create table finish_mark
(
    id int PRIMARY KEY
);
