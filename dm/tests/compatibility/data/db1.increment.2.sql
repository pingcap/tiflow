use compatibility;

insert into t1 (id, name, info) values (8, 'gentest', '{"id": 127}');
insert into t1 (id, name, info) values (9, 'gentest', '{"id": 128}');
update t1 set name = 'gentestxxxxxx' where id = 8;
delete from t1 where id = 9;
