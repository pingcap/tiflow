drop database if exists `common_1`;
create database `common_1`;
use `common_1`;

-- multi data type test

CREATE TABLE cdc_multi_data_type
(
    id          INT AUTO_INCREMENT,
    t_boolean   BOOLEAN,
    t_bigint    BIGINT,
    t_double    DOUBLE,
    t_decimal   DECIMAL(38, 19),
    t_bit       BIT(64),
    t_date      DATE,
    t_datetime  DATETIME,
    t_timestamp TIMESTAMP NULL,
    t_time      TIME,
    t_year      YEAR,
    t_char      CHAR,
    t_varchar   VARCHAR(10),
    t_blob      BLOB,
    t_text      TEXT,
    t_enum      ENUM ('enum1', 'enum2', 'enum3'),
    t_set       SET ('a', 'b', 'c'),
    t_json      JSON,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;

INSERT INTO cdc_multi_data_type( t_boolean, t_bigint, t_double, t_decimal, t_bit
                               , t_date, t_datetime, t_timestamp, t_time, t_year
                               , t_char, t_varchar, t_blob, t_text, t_enum
                               , t_set, t_json)
VALUES ( true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text', 'enum2'
       , 'a,b', NULL);

INSERT INTO cdc_multi_data_type( t_boolean, t_bigint, t_double, t_decimal, t_bit
                               , t_date, t_datetime, t_timestamp, t_time, t_year
                               , t_char, t_varchar, t_blob, t_text, t_enum
                               , t_set, t_json)
VALUES ( true, 9223372036854775807, 678, 321, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text', 'enum2'
       , 'a,b', NULL);

INSERT INTO cdc_multi_data_type(t_boolean)
VALUES (TRUE);

INSERT INTO cdc_multi_data_type(t_boolean)
VALUES (FALSE);

INSERT INTO cdc_multi_data_type(t_bigint)
VALUES (-9223372036854775808);

INSERT INTO cdc_multi_data_type(t_bigint)
VALUES (9223372036854775807);

INSERT INTO cdc_multi_data_type(t_json)
VALUES ('{
  "key1": "value1",
  "key2": "value2"
}');

-- view test

CREATE TABLE t1
(
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    c1 INT NOT NULL
);

INSERT INTO t1 (c1)
VALUES (1),
       (2),
       (3),
       (4),
       (5);

CREATE VIEW v1 AS
SELECT *
FROM t1
WHERE c1 > 2;

-- uk without pk
-- https://internal.pingcap.net/jira/browse/TOOL-714
-- CDC don't support UK is null

CREATE TABLE uk_without_pk
(
    id INT,
    a1 INT NOT NULL,
    a3 INT NOT NULL,
    UNIQUE KEY dex1 (a1, a3)
);

INSERT INTO uk_without_pk(id, a1, a3)
VALUES (1, 1, 2);

INSERT INTO uk_without_pk(id, a1, a3)
VALUES (2, 1, 1);

UPDATE uk_without_pk
SET id = 10,
    a1 = 2
WHERE a1 = 1;

UPDATE uk_without_pk
SET id = 100
WHERE a1 = 10;

UPDATE uk_without_pk
SET a3 = 4
WHERE a3 = 1;

-- bit column
-- Test issue: TOOL-1346

CREATE TABLE binlog_insert_bit
(
    a BIT(1) PRIMARY KEY,
    b BIT(64)
);

INSERT INTO binlog_insert_bit
VALUES (0x01, 0xffffffff);

UPDATE binlog_insert_bit
SET a = 0x00,
    b = 0xfffffffe;

-- recover test
-- Test issue: TOOL-1407
CREATE TABLE recover_and_insert
(
    id INT PRIMARY KEY,
    a  INT
);

INSERT INTO recover_and_insert(id, a)
VALUES (1, -1);

UPDATE recover_and_insert
SET a = -5
WHERE id = 1;

DROP TABLE recover_and_insert;

RECOVER TABLE recover_and_insert;

-- make sure we can insert data after recovery
INSERT INTO recover_and_insert(id, a)
VALUES (2, -3);

-- column null test

CREATE TABLE `column_is_null`
(
    `id` int(11) NOT NULL,
    `t`  datetime DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin;

INSERT INTO `column_is_null`(id)
VALUES (1),
       (2);
UPDATE `column_is_null`
SET t = NULL
WHERE id = 1;
