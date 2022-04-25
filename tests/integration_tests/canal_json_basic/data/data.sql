drop database if exists test;
create database test;
use test;

create table tp_int
(
<<<<<<< HEAD
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
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;

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
=======
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

-- unsigned int
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
values (1, 2, 3, 4, 5);

-- insert max value
insert into tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (255, 65535, 16777215, 4294967295, 18446744073709551615);

-- insert signed max value
insert into tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (127, 32767, 8388607, 2147483647, 9223372036854775807);

insert into tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (128, 32768, 8388608, 2147483648, 9223372036854775808);

update tp_unsigned_int set c_unsigned_int = 0, c_unsigned_tinyint = 0 where c_unsigned_smallint = 65535;
delete from tp_unsigned_int where c_unsigned_int = 0;

-- real
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

update tp_real set c_double = 2.333 where c_double = 2020.0303;

-- unsigned real
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

update tp_unsigned_real set c_unsigned_double = 2020.0404 where c_unsigned_double = 2020.0303;

-- time
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

insert into tp_time()
values ();

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020');

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2022-02-22', '2022-02-22 22:22:22', '2020-02-20 02:20:20', '02:20:20', '2021');

update tp_time set c_year = '2022' where c_year = '2020';
update tp_time set c_date = '2022-02-22' where c_datetime = '2020-02-20 02:20:20';

-- text
create table tp_text
(
    id           int auto_increment,
    c_tinytext   tinytext      null,
    c_text       text          null,
    c_mediumtext mediumtext    null,
    c_longtext   longtext      null,
    constraint pk
        primary key (id)
);

insert into tp_text()
values ();

insert into tp_text(c_tinytext, c_text, c_mediumtext, c_longtext)
values ('89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A');

insert into tp_text(c_tinytext, c_text, c_mediumtext, c_longtext)
values ('89504E470D0A1A0B', '89504E470D0A1A0B', '89504E470D0A1A0B', '89504E470D0A1A0B');

update tp_text set c_text = '89504E470D0A1A0B' where c_mediumtext = '89504E470D0A1A0A';

-- blob
create table tp_blob
(
    id           int auto_increment,
    c_tinyblob   tinyblob      null,
    c_blob       blob          null,
    c_mediumblob mediumblob    null,
    c_longblob   longblob      null,
    constraint pk
        primary key (id)
);

insert into tp_blob()
values ();

insert into tp_blob(c_tinyblob, c_blob, c_mediumblob, c_longblob)
values (x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A');

insert into tp_blob(c_tinyblob, c_blob, c_mediumblob, c_longblob)
values (x'89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B');

update tp_blob set c_blob = x'89504E470D0A1A0B' where c_mediumblob = x'89504E470D0A1A0A';

-- char / binary
create table tp_char_binary
(
    id           int auto_increment,
    c_char       char(16)      null,
    c_varchar    varchar(16)   null,
    c_binary     binary(16)    null,
    c_varbinary  varbinary(16) null,
    constraint pk
        primary key (id)
);

insert into tp_char_binary()
values ();

insert into tp_char_binary(c_char, c_varchar, c_binary, c_varbinary)
values ('89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A');

insert into tp_char_binary(c_char, c_varchar, c_binary, c_varbinary)
values ('89504E470D0A1A0B', '89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B');

update tp_char_binary set c_varchar = '89504E470D0A1A0B' where c_binary = x'89504E470D0A1A0A';

-- other
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

insert into tp_other()
values ();

insert into tp_other(c_enum, c_set, c_bit, c_json)
values ('a', 'a,b', b'1000001', '{
  "key1": "value1",
  "key2": "value2"
}');

insert into tp_other(c_enum, c_set, c_bit, c_json)
values ('b', 'b,c', b'1000001', '{
  "key1": "value1",
  "key2": "value2",
  "key3": "123"
}');

update tp_other set c_enum = 'c' where c_set = 'b, c';
>>>>>>> f085477fb (cdc/codec: canal-json decoder compatible with mysql sink, integrate with kafka consumer. (#4790))

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

create table finish_mark
(
    id int PRIMARY KEY
);
