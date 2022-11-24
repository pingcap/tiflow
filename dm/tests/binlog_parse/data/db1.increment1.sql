use binlog_parse;
insert into t1 (id, created_time) values (5, '2022-01-05 00:00:01'), (6, '2022-01-06 00:00:01');
insert into t2 (id, created_time) values (5, '2022-01-05 00:00:01'), (6, '2022-01-06 00:00:01');