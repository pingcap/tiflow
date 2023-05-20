DROP DATABASE IF EXISTS `lossy_ddl`;
CREATE DATABASE `lossy_ddl`;
USE `lossy_ddl`;

-- int -> unsigned int
CREATE TABLE example1
(
    id INT PRIMARY KEY,
    b  INT
);
INSERT INTO example1 (id, b)
VALUES (1, 1);
ALTER TABLE example1 MODIFY COLUMN b INT UNSIGNED;

-- int -> varchar
CREATE TABLE example2
(
    id INT PRIMARY KEY,
    b  INT
);
INSERT INTO example2 (id, b)
VALUES (1, 1);
ALTER TABLE example2 MODIFY COLUMN b VARCHAR (100);


-- timestamp -> datetime
CREATE TABLE example3
(
    id INT PRIMARY KEY,
    b  TIMESTAMP
);
INSERT INTO example3 (id, b)
VALUES (1, '2023-04-19 11:48:00');
ALTER TABLE example3 MODIFY COLUMN b DATETIME;

-- varchar(256) -> varchar(100)
CREATE TABLE example4
(
    id INT PRIMARY KEY,
    b  VARCHAR(256)
);
INSERT INTO example4 (id, b)
VALUES (1, '2023-04-19 11:48:00');
ALTER TABLE example4 MODIFY COLUMN b VARCHAR (100);

-- Drop column
CREATE TABLE example5
(
    id INT PRIMARY KEY,
    b  INT
);
INSERT INTO example5 (id, b)
VALUES (1, -1);
ALTER TABLE example5 DROP COLUMN b;

-- Add column
CREATE TABLE example6
(
    id INT PRIMARY KEY,
    b  INT
);
INSERT INTO example6 (id, b)
VALUES (1, -1);
ALTER TABLE example6
    ADD COLUMN c INT;

-- Modify collation
CREATE TABLE example7
(
    id INT PRIMARY KEY,
    b  VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_general_ci
);
INSERT INTO example7 (id, b)
VALUES (1, '2023-04-19 11:48:00');
ALTER TABLE example7 MODIFY COLUMN b VARCHAR (256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- Drop partition
CREATE TABLE example8
(
    id INT PRIMARY KEY,
    b  INT
) PARTITION BY RANGE (id) (
    PARTITION b0 VALUES LESS THAN (0),
    PARTITION b1 VALUES LESS THAN MAXVALUE
);
INSERT INTO example8 (id, b)
VALUES (-1, -1);
INSERT INTO example8 (id, b)
VALUES (1, 1);
ALTER TABLE example8 DROP PARTITION b0;

-- varchar(256) -> varchar(300)
CREATE TABLE example9
(
    id INT PRIMARY KEY,
    b  VARCHAR(256)
);
INSERT INTO example9 (id, b)
VALUES (1, '2023-04-19 11:48:00');
ALTER TABLE example9 MODIFY COLUMN b VARCHAR (300);


-- double -> float
CREATE TABLE example10
(
    id INT PRIMARY KEY,
    b  DOUBLE
);
INSERT INTO example10 (id, b)
VALUES (1, 1.0);
ALTER TABLE example10 MODIFY COLUMN b FLOAT;

-- bigint -> int
CREATE TABLE example11
(
    id BIGINT PRIMARY KEY,
    b  BIGINT
);
INSERT INTO example11 (id, b)
VALUES (1, 1);
ALTER TABLE example11 MODIFY COLUMN b INT;

-- longtext -> varchar(100)
CREATE TABLE example12
(
    id INT PRIMARY KEY,
    b  LONGTEXT
);
INSERT INTO example12 (id, b)
VALUES (1, '2023-04-19 11:48:00');
ALTER TABLE example12 MODIFY COLUMN b VARCHAR (100);

-- Enum('a', 'b', 'c') -> Enum('a', 'b')
CREATE TABLE example13
(
    id INT PRIMARY KEY,
    b  ENUM('a', 'b', 'c')
);
INSERT INTO example13 (id, b)
VALUES (1, 'a');
ALTER TABLE example13 MODIFY COLUMN b ENUM('a', 'b');

-- Set No STRICT_TRANS_TABLES
SET
@@SESSION.sql_mode = 'NO_ENGINE_SUBSTITUTION';

-- varchar(256) -> varchar(1) with a long value.
CREATE TABLE example14
(
    id INT PRIMARY KEY,
    b  VARCHAR(256)
);
INSERT INTO example14 (id, b)
VALUES (1, '2023-04-19 11:48:00');
ALTER TABLE example14 MODIFY COLUMN b VARCHAR (1);

-- int -> unsigned int with a negative value.
CREATE TABLE example15
(
    id INT PRIMARY KEY,
    b  INT
);
INSERT INTO example15 (id, b)
VALUES (1, -1);
ALTER TABLE example15 MODIFY COLUMN b INT UNSIGNED;

-- exchange partition
CREATE TABLE example16
(
    a INT PRIMARY KEY
) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
INSERT INTO example16
VALUES (1);
CREATE TABLE example17
(
    a INT PRIMARY KEY
);
INSERT INTO example17
VALUES (18);
ALTER TABLE example16 EXCHANGE PARTITION p2 WITH TABLE example17;


CREATE TABLE `finish_mark`
(
    id   INT PRIMARY KEY,
    name VARCHAR(20)
);
