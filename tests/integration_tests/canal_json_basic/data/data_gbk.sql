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
VALUES (1, '����', "�й�", "�Ϻ�", "���,����"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO cs_gbk
VALUES (2, '����', "����", "ŦԼ", "����,���"
	, 0xCAC0BDE7C4E3BAC3);

UPDATE cs_gbk
SET name = '����'
WHERE name = '����';

DELETE FROM cs_gbk
WHERE name = '����'
	AND country = '����'
	AND city = 'ŦԼ'
	AND description = '����,���';

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
	���� varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

ALTER TABLE test_ddl3
	ADD COLUMN ���� char(32);

ALTER TABLE test_ddl3
	MODIFY COLUMN ���� varchar(32);

ALTER TABLE test_ddl3
	DROP COLUMN ����;

/* this is a DDL test for table */
CREATE TABLE ��1 (
	id INT,
	name varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

RENAME TABLE ��1 TO ��2;

DROP TABLE ��2;

create table finish_mark
(
    id int PRIMARY KEY
);
