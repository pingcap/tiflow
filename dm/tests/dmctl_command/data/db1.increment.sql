use dmctl_command;
insert into t1 (id, name) values (3, 'Eddard Stark');
insert into t1 (id, name) values (4, 'xx');
update t1 set name='xx' where id=1;
delete from t1 where id=2;
-- this table will not be validated, since it has no pk
create table t1_2(id int);
insert into t1_2 values(1);
create table t_trigger_flush1(id int primary key)
