drop database if exists `common_1`;
create database `common_1`;
use `common_1`;

-- multi data type test

CREATE TABLE cdc_multi_data_type (
	id INT AUTO_INCREMENT,
	t_boolean BOOLEAN,
	t_bigint BIGINT,
	t_double DOUBLE,
	t_decimal DECIMAL(38,19),
	t_bit BIT(64),
	t_date DATE,
	t_datetime DATETIME,
	t_timestamp TIMESTAMP NULL,
	t_time TIME,
	t_year YEAR,
	t_char CHAR,
	t_varchar VARCHAR(10),
	t_blob BLOB,
	t_text TEXT,
	t_enum ENUM('enum1', 'enum2', 'enum3'),
	t_set SET('a', 'b', 'c'),
	t_json JSON,
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

INSERT INTO cdc_multi_data_type(t_boolean, t_bigint, t_double, t_decimal, t_bit
	,t_date, t_datetime, t_timestamp, t_time, t_year
	,t_char, t_varchar, t_blob, t_text, t_enum
	,t_set, t_json) VALUES
	(true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
	,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
	,'测', '测试', 'blob', '测试text', 'enum2'
	,'a,b', NULL);

INSERT INTO cdc_multi_data_type(t_boolean, t_bigint, t_double, t_decimal, t_bit
	,t_date, t_datetime, t_timestamp, t_time, t_year
	,t_char, t_varchar, t_blob, t_text, t_enum
	,t_set, t_json) VALUES
	(true, 9223372036854775807, 678, 321, b'1000001'
	,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
	,'测', '测试', 'blob', '测试text', 'enum2'
	,'a,b', NULL);

INSERT INTO cdc_multi_data_type(t_boolean) VALUES(TRUE);

INSERT INTO cdc_multi_data_type(t_boolean) VALUES(FALSE);

INSERT INTO cdc_multi_data_type(t_bigint) VALUES(-9223372036854775808);

INSERT INTO cdc_multi_data_type(t_bigint) VALUES(9223372036854775807);

INSERT INTO cdc_multi_data_type(t_json) VALUES('{"key1": "value1", "key2": "value2"}');

-- view test

CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, c1 INT NOT NULL);

INSERT INTO t1 (c1) VALUES (1),(2),(3),(4),(5);

CREATE VIEW v1 AS SELECT * FROM t1 WHERE c1 > 2;

-- mark finish table

CREATE TABLE finish_mark(a int primary key);