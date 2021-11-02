drop database if exists `expr_filter`;
create database `expr_filter`;
use `expr_filter`;
-- https://dev.mysql.com/doc/refman/8.0/en/data-types.html
create table t1 (
    id int NOT NULL AUTO_INCREMENT,
    should_skip int,

    c_null int,

    c_bit bit(8),
    c_tinyint tinyint(5) unsigned,
    c_smallint smallint(6),
    c_mediumint mediumint(7),
    c_int int(8),
    c_bigint bigint(9) unsigned,
    c_decimal decimal(7,2),
    c_float float(10),
    c_double double,

    c_date date,
    c_datetime datetime(3),
    c_timestamp timestamp(4),
    c_time time(5),
    c_year year,

    c_char char(4),
    c_varchar national varchar(255),
    c_binary binary(1),
    c_varbinary varbinary(4),
    c_tinyblob tinyblob,
    c_tinytext tinytext,
    c_blob blob(20),
    c_text text(10),
    c_mediumblob mediumblob,
    c_mediumtext mediumtext,
    c_longblob longblob,
    c_longtext longtext,
    c_enum enum('a', 'b', 'c'),
    c_set set('a', 'b', 'c'),

    c_json json,

    PRIMARY KEY (id)
);