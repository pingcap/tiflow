use binlog_parse;
insert into t1 (id, created_time) values (3, '2022-01-03 00:00:01'), (4, '2022-01-04 00:00:01');
insert into t2 (id, created_time) values (3, '2022-01-03 00:00:01'), (4, '2022-01-04 00:00:01');