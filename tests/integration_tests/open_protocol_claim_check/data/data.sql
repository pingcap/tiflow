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

insert into t values (
    1,
    1, 2, 3, 4, 5,
    1, 2, 3, 4, 5,
    2020.0202, 2020.0303, 2020.0404, 2021.1208,
    3.1415, 2.7182, 8000, 179394.233,
    '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
    '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
    x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    'b', 'b,c', b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
);

update t set c_float = 3.1415, c_double = 2.7182, c_decimal = 8000, c_decimal_2 = 179394.233 where id = 1;

delete from t where id = 1;

insert into t values (
     2,
     1, 2, 3, 4, 5,
     1, 2, 3, 4, 5,
     2020.0202, 2020.0303, 2020.0404, 2021.1208,
     3.1415, 2.7182, 8000, 179394.233,
     '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
     '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
     x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     'b', 'b,c', b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
);

update t set c_float = 3.1415, c_double = 2.7182, c_decimal = 8000, c_decimal_2 = 179394.233 where id = 2;

begin;

insert into t values (
     3,
     1, 2, 3, 4, 5,
     1, 2, 3, 4, 5,
     2020.0202, 2020.0303, 2020.0404, 2021.1208,
     3.1415, 2.7182, 8000, 179394.233,
     '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
     '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
     x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     'b', 'b,c', b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
 ),(
    4,
    1, 2, 3, 4, 5,
    1, 2, 3, 4, 5,
    2020.0202, 2020.0303, 2020.0404, 2021.1208,
    3.1415, 2.7182, 8000, 179394.233,
    '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
    '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
    x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    'b', 'b,c', b'1000001', '{
    "key1": "value1",
    "key2": "value2",
    "key3": "123"
  }'
),(
     5,
     1, 2, 3, 4, 5,
     1, 2, 3, 4, 5,
     2020.0202, 2020.0303, 2020.0404, 2021.1208,
     3.1415, 2.7182, 8000, 179394.233,
     '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
     '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
     x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     'b', 'b,c', b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
 );

commit;

create table finish_mark
(
    id int PRIMARY KEY
);
