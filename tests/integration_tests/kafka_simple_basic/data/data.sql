use test;

insert into tp_int() values ();

insert into tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

-- insert max value
insert into tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (127, 32767, 8388607, 2147483647, 9223372036854775807);

-- insert min value
insert into tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (-128, -32768, -8388608, -2147483648, -9223372036854775808);

-- not key changed update
update tp_int set c_int = 0, c_tinyint = 0 where c_smallint = 2;

delete from tp_int where c_int = 0;

rename table tp_int to tp_int2;
update tp_int2 set c_int = 5 where id = 2;

-- key changed update
update tp_int2 set id = 5 where id = 3;

insert into tp_unsigned_int() values ();

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



insert into tp_real() values ();

insert into tp_real(c_float, c_double, c_decimal, c_decimal_2)
values (2020.0202, 2020.0303, 2020.0404, 2021.1208);

insert into tp_real(c_float, c_double, c_decimal, c_decimal_2)
values (-2.7182818284, -3.1415926, -8000, -179394.233);

update tp_real set c_double = 2.333 where c_double = 2020.0303;

insert into tp_unsigned_real() values ();

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

insert into tp_time() values ();

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020');

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2022-02-22', '2022-02-22 22:22:22', '2020-02-20 02:20:20', '02:20:20', '2021');

update tp_time set c_year = '2022' where c_year = '2020';
update tp_time set c_date = '2022-02-22' where c_datetime = '2020-02-20 02:20:20';

alter table tp_time add column c_timestamp2 timestamp default current_timestamp;

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2024-03-09', '2022-02-22 22:22:22', '2020-02-20 02:20:20', '02:20:20', '2021');

alter table tp_time drop column c_timestamp2;

alter table tp_time add column c_timestamp2 timestamp default now();

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2024-03-09', '2022-02-22 22:22:22', '2020-02-20 02:20:20', '02:20:20', '2021');

alter table tp_time alter column c_timestamp2 drop default;

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year, c_timestamp2)
values ('2024-03-09', '2022-02-22 22:22:22', '2020-02-20 02:20:20', '02:20:20', '2021', "2024-03-09 18:00:00");

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

insert into tp_text() values ();

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

insert into tp_blob() values ();

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

insert into tp_char_binary() values ();

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
    c_bit  bit(10)            null,
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

update tp_other set c_enum = 'c' where c_set = 'b,c';

alter table tp_other modify column c_enum enum ('a', 'b', 'c','d') null;

insert into tp_other(c_enum, c_set, c_bit, c_json)
values ('d', 'b,c', b'1000001', '{
  "key1": "value1",
  "key2": "value2",
  "key3": "123"
}');

alter table tp_other modify column c_enum enum ('d', 'c', 'b', 'a') null;

update tp_other set c_enum = 'a' where c_enum = 'd';

alter table tp_other modify column c_set set ('a', 'b', 'c', 'd') null;

update tp_other set c_set = 'b,c' where c_set = 'a,b';

alter table tp_other modify column c_bit bit(20) null;

update tp_other set c_bit = b'1' where c_bit = b'1000001';

alter table tp_other modify column c_bit bit(1) null;

update tp_other set c_bit = b'0' where c_bit = b'1';

-- gbk dmls
CREATE TABLE cs_gbk (
	id INT,
	name varchar(128) CHARACTER SET gbk,
	country char(32) CHARACTER SET gbk,
	city varchar(64),
	description text CHARACTER SET gbk,
	image tinyblob,
	PRIMARY KEY (id)
) ENGINE = InnoDB CHARSET = utf8mb4;

INSERT INTO cs_gbk
VALUES (1, '测试', "中国", "上海", "你好,世界"
	, 0xC4E3BAC3CAC0BDE7);

INSERT INTO cs_gbk
VALUES (2, '部署', "美国", "纽约", "世界,你好"
	, 0xCAC0BDE7C4E3BAC3);

UPDATE cs_gbk
SET name = '开发'
WHERE name = '测试';

DELETE FROM cs_gbk
WHERE name = '部署'
	AND country = '美国'
	AND city = '纽约'
	AND description = '世界,你好';

-- ddls
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

CREATE TABLE test_ddl3 (
	id INT,
	名称 varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

ALTER TABLE test_ddl3
	ADD COLUMN 城市 char(32);

ALTER TABLE test_ddl3
	MODIFY COLUMN 城市 varchar(32);

ALTER TABLE test_ddl3
	DROP COLUMN 城市;

/* this is a DDL test for table */
CREATE TABLE 表1 (
	id INT,
	name varchar(128),
	PRIMARY KEY (id)
) ENGINE = InnoDB;

RENAME TABLE 表1 TO 表2;

DROP TABLE 表2;

create table finish_mark
(
    id int PRIMARY KEY
);
