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
--  FIXME: Currently canal-adapter does not handle year types correctly.
--  t_year               YEAR,
    t_enum               ENUM ('enum1', 'enum2', 'enum3'),
    t_set                SET ('a', 'b', 'c'),
--  FIXME: Currently there will be data inconsistencies.
--  t_bit                BIT(64),
    t_json               JSON,
    PRIMARY KEY (id)
);

-- make sure `nullable` can be handled by the mounter and mq encoding protocol
INSERT INTO multi_data_type() VALUES ();

INSERT INTO multi_data_type( t_tinyint, t_tinyint_unsigned, t_smallint, t_smallint_unsigned, t_mediumint
                           , t_mediumint_unsigned, t_int, t_int_unsigned, t_bigint, t_bigint_unsigned
                           , t_boolean, t_float, t_double, t_decimal
                           , t_char, t_varchar, c_binary, c_varbinary, t_tinytext, t_text, t_mediumtext, t_longtext
                           , t_tinyblob, t_blob, t_mediumblob, t_longblob
                           , t_date, t_datetime, t_timestamp, t_time
                           , t_enum
                           , t_set, t_json)
VALUES ( -1, 1, -129, 129, -65536, 65536, -16777216, 16777216, -2147483649, 2147483649
       , true, 123.456, 123.123, 123456789012.123456789012
       , '测', '测试', x'89504E470D0A1A0A', x'89504E470D0A1A0A', '测试tinytext', '测试text', '测试mediumtext', '测试longtext'
       , 'tinyblob', 'blob', 'mediumblob', 'longblob'
       , '1977-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59'
       , 'enum2'
       , 'a,b', NULL);

INSERT INTO multi_data_type( t_tinyint, t_tinyint_unsigned, t_smallint, t_smallint_unsigned, t_mediumint
                           , t_mediumint_unsigned, t_int, t_int_unsigned, t_bigint, t_bigint_unsigned
                           , t_boolean, t_float, t_double, t_decimal
                           , t_char, t_varchar, c_binary, c_varbinary, t_tinytext, t_text, t_mediumtext, t_longtext
                           , t_tinyblob, t_blob, t_mediumblob, t_longblob
                           , t_date, t_datetime, t_timestamp, t_time
                           , t_enum
                           , t_set, t_json)
VALUES ( -2, 2, -130, 130, -65537, 65537, -16777217, 16777217, -2147483650, 2147483650
       , false, 123.4567, 123.1237, 123456789012.1234567890127
       , '2', '测试2', x'89504E470D0A1A0B', x'89504E470D0A1A0B', '测试2tinytext', '测试2text', '测试2mediumtext', '测试longtext'
       , 'tinyblob2', 'blob2', 'mediumblob2', 'longblob2'
       , '2021-01-01', '2021-12-31 23:59:59', '19731230153000', '22:59:59'
       , 'enum1'
       , 'a,b,c', '{
    "id": 1,
    "name": "hello"
  }');

UPDATE multi_data_type
SET t_boolean = false
WHERE id = 1;

DELETE
FROM multi_data_type
WHERE id = 2;

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
