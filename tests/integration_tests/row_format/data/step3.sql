use `row_format`;

INSERT INTO multi_data_type( t_boolean, t_bigint, t_double, t_decimal, t_bit
                           , t_date, t_datetime, t_timestamp, t_time, t_year
                           , t_char, t_varchar, t_blob, t_text, t_enum
                           , t_set, t_json)
VALUES ( true, 9223372036875807, 153.123, 123456669012.123456789012, b'1010001'
       , '2000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text', 'enum1'
       , 'a,b', '{
    "key1": "value1",
    "key2": "value2"
  }');

UPDATE multi_data_type
SET t_bigint = 888,
    t_json   = '{
      "key0": "value0",
      "key2": "value2"
    }'
WHERE id = 2;

create table tp_int
(
    id          int auto_increment,
    c_tinyint   tinyint   null,
    c_smallint  smallint  null,
    c_mediumint mediumint null,
    c_int       int       null,
    c_bigint    bigint    null,
    constraint pk
        primary key (id)
);

insert into tp_int()
values ();

insert into tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

-- insert max value
insert into tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (127, 32767, 8388607, 2147483647, 9223372036854775807);

-- insert min value
insert into tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (-128, -32768, -8388608, -2147483648, -9223372036854775808);

update tp_int set c_int = 0, c_tinyint = 0 where c_smallint = 2;
delete from tp_int where c_int = 0;

create table tp_unsigned_int (
     id          int auto_increment,
     c_unsigned_tinyint   tinyint   unsigned null,
     c_unsigned_smallint  smallint  unsigned null,
     c_unsigned_mediumint mediumint unsigned null,
     c_unsigned_int       int       unsigned null,
     c_unsigned_bigint    bigint    unsigned null,
     constraint pk
         primary key (id)
);

insert into tp_unsigned_int()
values ();

insert into tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (1, 2, 3,
        4, 5);

-- insert max value
insert into tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (255, 65535, 16777215,
        4294967295, 18446744073709551615);

-- insert min value
insert into tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (0, 0, 0,
        0, 0);

update tp_unsigned_int set c_unsigned_int = 0, c_unsigned_tinyint = 0 where c_unsigned_smallint = 2;
delete from tp_unsigned_int where c_unsigned_int = 0;

create table tp_text
(
    id           int auto_increment,
    c_tinytext   tinytext      null,
    c_text       text          null,
    c_mediumtext mediumtext    null,
    c_longtext   longtext      null,
    c_varchar    varchar(16)   null,
    c_char       char(16)      null,
    c_tinyblob   tinyblob      null,
    c_blob       blob          null,
    c_mediumblob mediumblob    null,
    c_longblob   longblob      null,
    c_binary     binary(16)    null,
    c_varbinary  varbinary(16) null,
    constraint pk
        primary key (id)
);

create table tp_text2
(
    id           int auto_increment,
    c_tinytext   tinytext      null,
    c_text       text          null,
    c_mediumtext mediumtext    null,
    c_longtext   longtext      null,
    c_varchar    varchar(16)   default "a",
    c_char       char(16)      default "a",
    c_tinyblob   tinyblob      null,
    c_blob       blob          null,
    c_mediumblob mediumblob    null,
    c_longblob   longblob      null,
    c_binary     binary(16)    default '0xa',
    c_varbinary  varbinary(16) default '0xa',
    constraint pk
        primary key (id)
);

create table tp_time
(
    id          int auto_increment,
    c_date      date      null,
    c_datetime  datetime  null,
    c_timestamp timestamp null,
    c_time      time      null,
    c_year      year      null,
    constraint pk
        primary key (id)
);

create table tp_real
(
    id        int auto_increment,
    c_float   float   null,
    c_double  double  null,
    c_decimal decimal null,
    c_decimal_2 decimal(10, 4) null,
    constraint pk
        primary key (id)
);

insert into tp_real()
values ();

insert into tp_real(c_float, c_double, c_decimal, c_decimal_2)
values (2020.0202, 2020.0303, 2020.0404, 2021.1208);

insert into tp_real(c_float, c_double, c_decimal, c_decimal_2)
values (-2.7182818284, -3.1415926, -8000, -179394.233);

create table tp_unsigned_real (
      id                   int auto_increment,
      c_unsigned_float     float unsigned   null,
      c_unsigned_double    double unsigned  null,
      c_unsigned_decimal   decimal unsigned null,
      c_unsigned_decimal_2 decimal(10, 4) unsigned null,
      constraint pk
          primary key (id)
);

insert into tp_unsigned_real()
values ();

insert into tp_unsigned_real(c_unsigned_float, c_unsigned_double, c_unsigned_decimal, c_unsigned_decimal_2)
values (2020.0202, 2020.0303, 2020.0404, 2021.1208);

create table tp_other
(
    id     int auto_increment,
    c_enum enum ('a','b','c') null,
    c_set  set ('a','b','c')  null,
    c_bit  bit(64)            null,
    c_json json               null,
    constraint pk
        primary key (id)
);

insert into tp_text()
values ();

insert into tp_text(c_tinytext, c_text, c_mediumtext, c_longtext, c_varchar, c_char, c_tinyblob, c_blob, c_mediumblob,
                    c_longblob, c_binary, c_varbinary)
values ('89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
        '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A'
           , x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A'),
('', '', '', '', '', '', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
 x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A'),
('89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', x'', x'', x'',
 x'', x'', x'');

insert into tp_text2() values();

insert into tp_time()
values ();

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020');

insert into tp_other()
values ();

insert into tp_other(c_enum, c_set, c_bit, c_json)
values ('a', 'a,b', b'1000001', '{
  "key1": "value1",
  "key2": "value2"
}');
