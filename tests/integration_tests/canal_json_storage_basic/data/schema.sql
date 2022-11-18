USE `test`;

CREATE TABLE multi_data_type
(
    id                   INT AUTO_INCREMENT,
    t_tinyint            TINYINT,
    t_tinyint_unsigned   TINYINT UNSIGNED,
    t_smallint           SMALLINT,
    t_smallint_unsigned  SMALLINT UNSIGNED,
    t_mediumint          MEDIUMINT,
    t_mediumint_unsigned MEDIUMINT UNSIGNED,
    t_int                INT,
    t_int_unsigned       INT UNSIGNED,
    t_bigint             BIGINT,
    t_bigint_unsigned    BIGINT UNSIGNED,
    t_boolean            BOOLEAN,
    t_float              FLOAT(6, 2),
    t_double             DOUBLE(6, 2),
    t_decimal            DECIMAL(38, 19),
    t_char               CHAR,
    t_varchar            VARCHAR(10),
    c_binary             binary(16),
    c_varbinary          varbinary(16),
    t_tinytext           TINYTEXT,
    t_text               TEXT,
    t_mediumtext         MEDIUMTEXT,
    t_longtext           LONGTEXT,
    t_tinyblob           TINYBLOB,
    t_blob               BLOB,
    t_mediumblob         MEDIUMBLOB,
    t_longblob           LONGBLOB,
    t_date               DATE,
    t_datetime           DATETIME,
    t_timestamp          TIMESTAMP NULL,
    t_time               TIME,
--  t_year               YEAR,
    t_enum               ENUM ('enum1', 'enum2', 'enum3'),
    t_set                SET ('a', 'b', 'c'),
--  t_bit                BIT(64),
    t_json               JSON,
    PRIMARY KEY (id)
);

CREATE TABLE multi_charset (
	id INT,
	name varchar(128) CHARACTER SET gbk,
	country char(32) CHARACTER SET gbk,
	city varchar(64),
	description text CHARACTER SET gbk,
	image tinyblob,
	PRIMARY KEY (id)
) ENGINE = InnoDB CHARSET = utf8mb4;

CREATE TABLE binary_columns
(
    id                   INT AUTO_INCREMENT,
    c_binary             binary(255),
    c_varbinary          varbinary(255),
    t_tinyblob           TINYBLOB,
    t_blob               BLOB,
    t_mediumblob         MEDIUMBLOB,
    t_longblob           LONGBLOB,
    PRIMARY KEY (id)
);