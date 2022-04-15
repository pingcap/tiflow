use `gbk`;
insert into t1 (id, name) values (3, '你好3'), (4, '你好4');
update t1 set name = '你好5' where id = 1;
delete from t1 where id = 2;

create table t2 (id int, name varchar(20), primary key(`id`)) character set gbk;
insert into t2 (id, name) values (1, '你好1'), (2, '你好2');
insert into t2 (id, name) values (3, '你好3'), (4, '你好4');
update t2 set name = '你好5' where id = 1;
delete from t2 where id = 2;

create table t4 (id int, name varchar(20) character set gbk, primary key(`id`)) character set utf8;
insert into t4 (id, name) values (1, '你好1'), (2, '你好2');
insert into t4 (id, name) values (3, '你好3'), (4, '你好4');
update t4 set name = '你好5' where id = 1;
delete from t4 where id = 2;
