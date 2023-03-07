DROP DATABASE IF EXISTS `charset_gbk_test0`;

CREATE DATABASE `charset_gbk_test0` CHARACTER SET utf8mb4;

USE `charset_gbk_test0`;

/* this is a test for columns which charset is gbk, with pk*/
CREATE TABLE t0 (
	id INT,
	name varchar(128) CHARACTER SET gbk,
	country char(32) CHARACTER SET gbk,
	city varchar(64),
	description text CHARACTER SET gbk,
	image tinyblob,
	PRIMARY KEY (id)
) ENGINE = InnoDB CHARSET = utf8mb4;

INSERT INTO t0
VALUES (1, '测试', "中国", "上海", "你好,世界"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO t0
VALUES (2, '部署', "美国", "纽约", "世界,你好"
	, 0xCAC0BDE7C4E3BAC3);

UPDATE t0
SET name = '开发'
WHERE name = '测试';

DELETE FROM t0
WHERE name = '部署';

/* this is a test for table which charset is gbk, without pk but with uk */
CREATE TABLE t1 (
	id INT NOT NULL,
	name varchar(128) CHARACTER SET gbk NOT NULL,
	country char(32) CHARACTER SET gbk,
	city varchar(64),
	description text CHARACTER SET gbk,
	image tinyblob,
	UNIQUE KEY (id, name)
) ENGINE = InnoDB CHARSET = utf8mb4;

INSERT INTO t1
VALUES (1, '测试', "中国", "上海", "你好,世界"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO t1
VALUES (2, '部署', "美国", "纽约", "世界,你好"
	, 0xCAC0BDE7C4E3BAC3);

UPDATE t1
SET name = '开发'
WHERE name = '测试';

DELETE FROM t1
WHERE name = '部署';

/* this is a test for table which charset is gbk*/
CREATE TABLE t2 (
	id INT,
	name varchar(128),
	country char(32),
	city varchar(64),
	description text,
	image tinyblob,
	PRIMARY KEY (id)
) ENGINE = InnoDB CHARSET = gbk;

INSERT INTO t2
VALUES (1, '测试', "中国", "上海", "你好,世界"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO t2
VALUES (2, '部署', "美国", "纽约", "世界,你好"
	, 0xCAC0BDE7C4E3BAC3);

UPDATE t2
SET name = '开发'
WHERE name = '测试';

DELETE FROM t2
WHERE name = '部署';

/* this is a test for db which charset is gbk*/
DROP DATABASE IF EXISTS `charset_gbk_test1`;

CREATE DATABASE `charset_gbk_test1` CHARACTER SET GBK;

USE `charset_gbk_test1`;

CREATE TABLE t0 (
	id INT,
	name varchar(128),
	country char(32),
	city varchar(64),
	description text,
	image tinyblob,
	PRIMARY KEY (id)
) ENGINE = InnoDB;

INSERT INTO t0
VALUES (1, '测试', "中国", "上海", "你好,世界"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO t0
VALUES (2, '部署', "美国", "纽约", "世界,你好"
	, 0xCAC0BDE7C4E3BAC3);

UPDATE t0
SET name = '开发'
WHERE name = '测试'
	AND country = '中国'
	AND description = '你好,世界';

DELETE FROM t0
WHERE name = '部署'
	AND country = '美国'
	AND description = '世界,你好';

/* this is a DLL test for column */
CREATE TABLE t1 (
	id INT,
	名称 varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

ALTER TABLE t1
	ADD COLUMN 城市 char(32);

ALTER TABLE t1
	MODIFY COLUMN 城市 varchar(32);

ALTER TABLE t1
	DROP COLUMN 城市;

/* this is a DDL test for table */
CREATE TABLE 表2 (
	id INT,
	name varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

RENAME TABLE 表2 TO 表3;

DROP TABLE 表3;

/* this is a DDL test for database */
DROP DATABASE IF EXISTS `测试库`;

CREATE DATABASE `测试库` CHARACTER SET GBK;

DROP DATABASE `测试库`;

USE `test`;

CREATE TABLE finish_mark (
	id int PRIMARY KEY
);
