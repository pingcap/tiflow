create database if not exists json_test;
create table json_test.test (a int, b json, primary key(a));

insert into json_test.test values (1, '{"id": 1, "name":"aaa"}');
insert into json_test.test values (2, '{"id": 2, "name":"bbb", "sub": {"id": "2-1", "num": 3, "array": ["123", "456", "789"], "num_array": [123, 456, 789]}}');
insert into json_test.test values (3, '{"name":"ccc", "id": 3}');
insert into json_test.test values (4, '{"id": 4, "bool": true, "name":"aaa"}');
