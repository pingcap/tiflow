SET sql_mode='strict_trans_tables';
SET time_zone='+06:00';

/*
----------------------------------------------------------------------
-- DATE type
----------------------------------------------------------------------
*/

CREATE TABLE t_date(
  col     DATE,
  pk      INT PRIMARY KEY
);

INSERT INTO t_date VALUES ('2023-11-16', 1);
INSERT INTO t_date VALUES ('1000-01-01', 2);
INSERT INTO t_date VALUES ('9999-12-31', 3);

SET sql_mode='';
INSERT INTO t_date VALUES (/* Zero dates */ '0000-00-00', 4);
INSERT INTO t_date VALUES (/* Invalid dates */ '2009-11-31', 5);
SET sql_mode='strict_trans_tables';

/*
----------------------------------------------------------------------
-- DATETIME type
----------------------------------------------------------------------
*/

CREATE TABLE t_datetime(
  col     DATETIME,
  col_0   DATETIME(0),
  col_1   DATETIME(1),
  col_2   DATETIME(2),
  col_3   DATETIME(3),
  col_4   DATETIME(4),
  col_5   DATETIME(5),
  col_6   DATETIME(6),
  col_z   DATETIME DEFAULT 0,
  col_default_current_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  pk      INT PRIMARY KEY
);

INSERT INTO t_datetime VALUES (
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  NULL,
  '2023-11-16 12:34:56.123456',
  1
);

INSERT INTO t_datetime VALUES (
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  NULL,
  '2023-11-16 12:34:56',
  2
);

SET time_zone='+04:00';

INSERT INTO t_datetime VALUES (
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  NULL,
  '2023-11-16 12:34:56.123456',
  3
);

SET time_zone='+06:00';

INSERT INTO t_datetime VALUES (
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  4
);

/*
----------------------------------------------------------------------
-- TIME type
----------------------------------------------------------------------
*/

CREATE TABLE t_time(
  col     TIME,
  col_0   TIME(0),
  col_1   TIME(1),
  col_5   TIME(5),
  col_6   TIME(6),
  pk      INT PRIMARY KEY
);

INSERT INTO t_time VALUES (
  '00:00:00',
  '00:00:00',
  '00:00:00',
  '00:00:00',
  '00:00:00',
  1
);

INSERT INTO t_time VALUES (
  '00:00:00.123456',
  '00:00:00.123456',
  '00:00:00.123456',
  '00:00:00.123456',
  '00:00:00.123456',
  2
);

INSERT INTO t_time VALUES (
  '-10:23:45.123456',
  '-10:23:45.123456',
  '-10:23:45.123456',
  '-10:23:45.123456',
  '-10:23:45.123456',
  3
);

INSERT INTO t_time VALUES (
  '838:59:59.000000',
  '838:59:59.000000',
  '838:59:59.000000',
  '838:59:59.000000',
  '838:59:59.000000',
  4
);

INSERT INTO t_time VALUES (
  '123:44:56.123456',
  '123:44:56.123456',
  '123:44:56.123456',
  '123:44:56.123456',
  '123:44:56.123456',
  5
);

/*

Commented out because Debezium produce wrong result:

"col":-3020399000000,
"col_0":-3020399000000,
"col_1":-3020400147483,
"col_5":-3020399048576,
"col_6":-3020399048576,
INSERT INTO t_time VALUES (
  '-838:59:59.000000',
  '-838:59:59.000000',
  '-838:59:59.000000',
  '-838:59:59.000000',
  '-838:59:59.000000',
  6
);
*/

/*
----------------------------------------------------------------------
-- TIMESTAMP type
----------------------------------------------------------------------
*/

CREATE TABLE t_timestamp(
  col     TIMESTAMP,
  col_0   TIMESTAMP(0),
  col_1   TIMESTAMP(1),
  col_5   TIMESTAMP(5),
  col_6   TIMESTAMP(6),
  col_z   TIMESTAMP DEFAULT 0,
  pk      INT PRIMARY KEY
);

INSERT INTO t_timestamp VALUES (
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  NULL,
  1
);

INSERT INTO t_timestamp VALUES (
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  '2023-11-16 12:34:56',
  NULL,
  2
);

SET time_zone='+04:00';

INSERT INTO t_timestamp VALUES (
  '2023-11-16 12:34:56.123456',
  FROM_UNIXTIME(1),
  FROM_UNIXTIME(1470762668),
  '2023-11-16 12:34:56.123456',
  '2023-11-16 12:34:56.123456',
  NULL,
  3
);

SET time_zone='+06:00';

/*
----------------------------------------------------------------------
-- YEAR type
----------------------------------------------------------------------
*/

CREATE TABLE t_year(
  col     YEAR,
  col_4   YEAR(4),
  pk      INT PRIMARY KEY
);

INSERT INTO t_year VALUES (1901, 1901, 1);

/*
----------------------------------------------------------------------
-- BIT type
----------------------------------------------------------------------
*/

CREATE TABLE t_bit(
  col_1   BIT(1),
  col_5   BIT(5),
  col_6   BIT(6),
  col_60  BIT(60),
  pk      INT PRIMARY KEY
);

INSERT INTO t_bit VALUES (0, 16, 16, 16, 1);
INSERT INTO t_bit VALUES (1, 1, 1, 1, 2);

/*
----------------------------------------------------------------------
-- VARCHAR type
----------------------------------------------------------------------
*/

CREATE TABLE t_varchar(
  col           VARCHAR(64),
  col_utf8_bin  VARCHAR(64)
                CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  /*

  Note: There is a bug of Debezium that it does not recognize
  VARCHAR(..) CHARACTER SET BINARY as binary type
  when the table is created AFTER the connector.

  col_bin       VARCHAR(64)
                CHARACTER SET BINARY,
  */

  pk            INT PRIMARY KEY
);

INSERT INTO t_varchar VALUES ('abc', 'abc', /* 'abc' , */ 1);
INSERT INTO t_varchar VALUES ('def', 'def', /* 0xAABBCC , */ 2);

/*
----------------------------------------------------------------------
-- BINARY / VARBINARY type
----------------------------------------------------------------------
*/

CREATE TABLE t_binary(
  col_1   BINARY(64),
  col_2   VARBINARY(64),
  pk      INT PRIMARY KEY
);

INSERT INTO t_binary VALUES ('abc', 'abc', 1);
INSERT INTO t_binary VALUES ('def', 'def', 2);

/*
----------------------------------------------------------------------
-- BLOB / TEXT type
----------------------------------------------------------------------
*/

CREATE TABLE t_blob(
  col_b   BLOB,
  col_t   TEXT,
  pk      INT PRIMARY KEY
);

INSERT INTO t_blob VALUES ('abc', 'abc', 1);

/*
----------------------------------------------------------------------
-- BOOL type
----------------------------------------------------------------------
*/

CREATE TABLE t_bool(
  /*

  We do not support BOOL type.
  Debezium supports BOOL type only when the table is created AFTER the connector.

  col_bool        BOOL,

  */

  col_tinyint_1   TINYINT(1),
  col_tinyint_1_u TINYINT(1) UNSIGNED,
  col_tinyint_2   TINYINT(2),
  pk              INT PRIMARY KEY
);

INSERT INTO t_bool VALUES(/* true, */ 10, 10, 10, 1);
INSERT INTO t_bool VALUES(/* false, */ 10, 10, 10, 2);


/*
----------------------------------------------------------------------
-- FLOAT / DOUBLE type
----------------------------------------------------------------------
*/

CREATE TABLE t_float(
  col_f   FLOAT,
  col_d   DOUBLE,
  pk     INT PRIMARY KEY
);

INSERT INTO t_float VALUES (12345.12345, 12345.12345, 1);

/*
----------------------------------------------------------------------
-- DECIMAL / NUMERIC type
----------------------------------------------------------------------
*/

CREATE TABLE t_decimal(
  col_d  DECIMAL(10, 5),
  col_n  NUMERIC(10, 5),
  pk     INT PRIMARY KEY
);

INSERT INTO t_decimal VALUES (12345.12345, 12345.12345, 1);

/*
----------------------------------------------------------------------
-- JSON type
----------------------------------------------------------------------
*/

CREATE TABLE t_json(
  col     JSON,
  pk      INT PRIMARY KEY
);

INSERT INTO t_json VALUES ('["foo"]', 1);

/*
----------------------------------------------------------------------
-- ENUM / SET type
----------------------------------------------------------------------
*/

CREATE TABLE t_enum(
  col_e   ENUM('a', 'b', 'c'),
  col_s   SET('a', 'b', 'c'),
  pk      INT PRIMARY KEY
);

INSERT INTO t_enum VALUES ('a', 'c', 1);

SET sql_mode='';
INSERT INTO t_enum VALUES ('d', 'e', 2);
SET sql_mode='strict_trans_tables';
