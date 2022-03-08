DROP DATABASE IF EXISTS `charset_gbk_test0`;

CREATE DATABASE `charset_gbk_test0` CHARACTER SET utf8mb4;

USE `charset_gbk_test0`;

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

CREATE TABLE t1 (
	id INT,
	name varchar(128),
	country char(32),
	city varchar(64),
	description text,
	image tinyblob,
	PRIMARY KEY (id)
) ENGINE = InnoDB CHARSET = gbk;

INSERT INTO t1
VALUES (1, '测试', "中国", "上海", "你好,世界"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO t1
VALUES (2, '部署', "美国", "纽约", "世界,你好"
	, 0xCAC0BDE7C4E3BAC3);

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