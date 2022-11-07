use `test`;
-- make sure `nullable` can be handled properly.
INSERT INTO multi_data_type() VALUES ();

INSERT INTO multi_data_type( t_tinyint, t_tinyint_unsigned, t_smallint, t_smallint_unsigned, t_mediumint
                           , t_mediumint_unsigned, t_int, t_int_unsigned, t_bigint, t_bigint_unsigned
                           , t_boolean, t_float, t_double, t_decimal
                           , t_char, t_varchar, c_binary, c_varbinary, t_tinytext, t_text, t_mediumtext, t_longtext
                           , t_tinyblob, t_blob, t_mediumblob, t_longblob
                           , t_date, t_datetime, t_timestamp, t_time, t_year
                           , t_enum, t_bit
                           , t_set, t_json)
VALUES ( -1, 1, -129, 129, -65536, 65536, -16777216, 16777216, -2147483649, 2147483649
       , true, 123.456, 123.123, 123456789012.123456789012
       , '测', '测试', x'89504E470D0A1A0A', x'89504E470D0A1A0A', '测试tinytext', '测试text', '测试mediumtext', '测试longtext'
       , 'tinyblob', 'blob', 'mediumblob', 'longblob'
       , '1977-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 2022
       , 'enum2', 1
       , 'a,b', NULL);

INSERT INTO multi_data_type( t_tinyint, t_tinyint_unsigned, t_smallint, t_smallint_unsigned, t_mediumint
                           , t_mediumint_unsigned, t_int, t_int_unsigned, t_bigint, t_bigint_unsigned
                           , t_boolean, t_float, t_double, t_decimal
                           , t_char, t_varchar, c_binary, c_varbinary, t_tinytext, t_text, t_mediumtext, t_longtext
                           , t_tinyblob, t_blob, t_mediumblob, t_longblob
                           , t_date, t_datetime, t_timestamp, t_time, t_year
                           , t_enum, t_bit
                           , t_set, t_json)
VALUES ( -2, 2, -130, 130, -65537, 65537, -16777217, 16777217, -2147483650, 2147483650
       , false, 123.4567, 123.1237, 123456789012.1234567890127
       , '2', '测试2', x'89504E470D0A1A0B', x'89504E470D0A1A0B', '测试2tinytext', '测试2text', '测试2mediumtext', '测试longtext'
       , 'tinyblob2', 'blob2', 'mediumblob2', 'longblob2'
       , '2021-01-01', '2021-12-31 23:59:59', '19731230153000', '22:59:59', 2021
       , 'enum1', 2,
       , 'a,b,c', '{
    "id": 1,
    "name": "hello"
  }');

UPDATE multi_data_type
SET t_boolean = false
WHERE id = 1;

DELETE
FROM multi_data_type
WHERE id = 3;