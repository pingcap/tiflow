use compatibility;

insert into t1 (id, name, info) values (10, 'gentest', '{"id": 129}');
insert into t1 (id, name, info) values (11, 'gentest', '{"id": 130}');
update t1 set name = 'gentestxxxxxx' where id = 10;
delete from t1 where id = 11;
