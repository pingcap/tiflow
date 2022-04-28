use dmctl_command;
insert into t1 (id, name) values (3, 'Eddard Stark');
insert into t1 (id, name) values (4, 'xx');
update t1 set name='xx' where id=1;
delete from t1 where id=2;
create table t_trigger_flush1(id int primary key)
