use `gbk2`;
insert into t1 (id, name) values (3, '你好aa3'), (4, '你好AA4');
update t1 set name = '你好Aa5' where id = 1;
delete from t1 where id = 2;

create table t2 (id int, name varchar(20), primary key(`id`)) character set utf8;
insert into t2 (id, name) values (1, '你好Aa'), (2, '你好aA');
insert into t2 (id, name) values (3, '你好aa3'), (4, '你好AA4');
update t2 set name = '你好Aa5' where id = 1;
delete from t2 where id = 2;
