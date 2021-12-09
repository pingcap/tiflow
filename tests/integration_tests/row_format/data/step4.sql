use `row_format`;

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

insert into tp_text()
values ();

insert into tp_text(c_tinytext, c_text, c_mediumtext, c_longtext, c_varchar, c_char, c_tinyblob, c_blob, c_mediumblob,
                    c_longblob, c_binary, c_varbinary)
values ('89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
        '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A'
           , x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A');

insert into tp_text2() values();

insert into tp_time()
values ();

insert into tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020');

insert into tp_real()
values ();

insert into tp_real(c_float, c_double, c_decimal)
values (2020.0202, 2020.0303, 2020.0404);

insert into tp_other()
values ();

insert into tp_other(c_enum, c_set, c_bit, c_json)
values ('a', 'a,b', b'1000001', '{
  "key1": "value1",
  "key2": "value2"
}');


create table finish_mark
(
    id int PRIMARY KEY
);
