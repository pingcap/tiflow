use incompatible_ddl_changes;
insert into t1 (should_skip, c_null) values (0, NULL), (1, 123);

insert into t1 (should_skip, c_bit) values (0, b'1010'), (1, b'1111');
insert into t1 (should_skip, c_tinyint) values (0, 5), (1, 25);
insert into t1 (should_skip, c_smallint) values (0, 6), (1, 26);
insert into t1 (should_skip, c_mediumint) values (0, 7), (1, 27);
insert into t1 (should_skip, c_int) values (0, 8), (1, 28);
insert into t1 (should_skip, c_bigint) values (0, 9), (1, 29);
insert into t1 (should_skip, c_decimal) values (0, 10.01), (1, 30.02);
insert into t1 (should_skip, c_float) values (0, 11.12), (1, 31.324);
insert into t1 (should_skip, c_double) values (0, 12.32), (1, 32.4334);

insert into t1 (should_skip, c_date) values (0, '1995-03-07'), (1, '2021-06-22');
insert into t1 (should_skip, c_datetime) values (0, '1000-01-01 12:34:56.789'), (1, '2021-01-01 12:34:56.789');
set time_zone = '+08:00';
insert into t1 (should_skip, c_timestamp) values (0, '2020-01-01 12:34:56.1234'), (1, '2021-01-01 12:34:56.5678');
insert into t1 (should_skip, c_time) values (0, '-838:59:50.12345'), (1, '-838:58:50.12345');
insert into t1 (should_skip, c_year) values (0, 2020), (1, 2021);

insert into t1 (should_skip, c_char) values (0, '1234'), (1, '2234');
insert into t1 (should_skip, c_varchar) values (0, '3234'), (1, '4234');
insert into t1 (should_skip, c_binary) values (0, 'a'), (1, 'b');
insert into t1 (should_skip, c_varbinary) values (0, 'c'), (1, 'd');
insert into t1 (should_skip, c_tinyblob) values (0, 'a'), (1, 'b');
insert into t1 (should_skip, c_tinytext) values (0, 'c'), (1, 'd');
insert into t1 (should_skip, c_blob) values (0, 'qwer'), (1, 'asdf');
insert into t1 (should_skip, c_text) values (0, 'qwe'), (1, 'asd');
insert into t1 (should_skip, c_mediumblob) values (0, 'qwer'), (1, 'asdf');
insert into t1 (should_skip, c_mediumtext) values (0, 'qwer'), (1, 'asdf');
insert into t1 (should_skip, c_longblob) values (0, 'qwer'), (1, 'asdf');
insert into t1 (should_skip, c_longtext) values (0, 'qwer'), (1, 'asdf');
insert into t1 (should_skip, c_enum) values (0, 'a'), (1, 'b');
insert into t1 (should_skip, c_set) values (0, 'a'), (1, 'b,c');

insert into t1 (should_skip, c_json) values (0, '{"id": 1}'), (1, '{"id": 2}');
-- trigger a flush
alter table t1 add column dummy int;