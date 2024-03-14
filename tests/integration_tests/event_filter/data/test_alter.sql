USE `event_filter`;

/* test  */
ALTER TABLE t_alter MODIFY t_bigint BIGINT;

-- make sure `nullable` can be handled by the mounter and mq encoding protocol
INSERT INTO t_alter() VALUES ();

INSERT INTO t_alter( t_boolean, t_bigint, t_double, t_decimal, t_bit
                           , t_date, t_datetime, t_timestamp, t_time, t_year
                           , t_char, t_varchar, t_blob, t_text, t_enum
                           , t_set, t_json)
VALUES ( true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text', 'enum2'
       , 'a,b', NULL);

INSERT INTO t_alter( t_boolean, t_bigint, t_double, t_decimal, t_bit
                           , t_date, t_datetime, t_timestamp, t_time, t_year
                           , t_char, t_varchar, t_blob, t_text, t_enum
                           , t_set, t_json)
VALUES ( false, 666, 123.777, 123456789012.123456789012, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text11', 'enum3'
       , 'a,b', NULL);

UPDATE t_alter
SET t_bigint = 555
WHERE id = 1;

INSERT INTO t_alter( t_boolean, t_bigint, t_double, t_decimal, t_bit
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

UPDATE t_alter
SET t_bigint = 888,
    t_json   = '{
      "key0": "value0",
      "key2": "value2"
    }'
WHERE id = 2;
