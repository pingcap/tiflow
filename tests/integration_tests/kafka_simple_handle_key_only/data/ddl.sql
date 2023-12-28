drop database if exists test;
create database test;
use test;

create table t (
    id0          int primary key auto_increment,

    c_tinyint1   tinyint   null,
    c_smallint2  smallint  null,
    c_mediumint3 mediumint null,
    c_int4       int       null,
    c_bigint5    bigint    null,

    c_unsigned_tinyint6   tinyint   unsigned null,
    c_unsigned_smallint7  smallint  unsigned null,
    c_unsigned_mediumint8 mediumint unsigned null,
    c_unsigned_int9       int       unsigned null,
    c_unsigned_bigint10    bigint    unsigned null,

    c_float11   float   null,
    c_double12  double  null,
    c_decimal13 decimal null,
    c_decimal_214 decimal(10, 4) null,

    c_unsigned_float15     float unsigned   null,
    c_unsigned_double16    double unsigned  null,
    c_unsigned_decimal17   decimal unsigned null,
    c_unsigned_decimal_218 decimal(10, 4) unsigned null,

    c_date19      date      null,
    c_datetime20  datetime  null,
    c_timestamp21 timestamp null,
    c_time22      time      null,
    c_year23      year      null,

    c_tinytext24   tinytext      null,
    c_text25       text          null,
    c_mediumtext26 mediumtext    null,
    c_longtext27   longtext      null,

    c_tinyblob28   tinyblob      null,
    c_blob29       blob          null,
    c_mediumblob30 mediumblob    null,
    c_longblob31   longblob      null,

    c_char32       char(16)      null,
    c_varchar33    varchar(16)   null,
    c_binary34     binary(16)    null,
    c_varbinary35  varbinary(16) null,

    c_enum36 enum ('a','b','c') null,
    c_set37  set ('a','b','c')  null,
    c_bit38  bit(64)            null,
    c_json39 json               null
);
