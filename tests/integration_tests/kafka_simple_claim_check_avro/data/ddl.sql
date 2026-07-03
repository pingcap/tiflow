drop database if exists test;
create database test;
use test;

create table t (
    id          int primary key auto_increment,

    c_tinyint   tinyint   null,
    c_smallint  smallint  null,
    c_mediumint mediumint null,
    c_int       int       null,
    c_bigint    bigint    null,

    c_unsigned_tinyint   tinyint   unsigned null,
    c_unsigned_smallint  smallint  unsigned null,
    c_unsigned_mediumint mediumint unsigned null,
    c_unsigned_int       int       unsigned null,
    c_unsigned_bigint    bigint    unsigned null,

    c_float   float   null,
    c_double  double  null,
    c_decimal decimal null,
    c_decimal_2 decimal(10, 4) null,

    c_unsigned_float     float unsigned   null,
    c_unsigned_double    double unsigned  null,
    c_unsigned_decimal   decimal unsigned null,
    c_unsigned_decimal_2 decimal(10, 4) unsigned null,

    c_date      date      null,
    c_datetime  datetime  null,
    c_timestamp timestamp null,
    c_time      time      null,
    c_year      year      null,

    c_tinytext   tinytext      null,
    c_text       text          null,
    c_mediumtext mediumtext    null,
    c_longtext   longtext      null,

    c_tinyblob   tinyblob      null,
    c_blob       blob          null,
    c_mediumblob mediumblob    null,
    c_longblob   longblob      null,

    c_char       char(16)      null,
    c_varchar    varchar(16)   null,
    c_binary     binary(16)    null,
    c_varbinary  varbinary(16) null,

    c_enum enum ('a','b','c') null,
    c_set  set ('a','b','c')  null,
    c_bit  bit(64)            null,
    c_json json               null
);
