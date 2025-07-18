DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;

create table t1 (id int primary key, account_id int not null);
alter table t1 add unique key(account_id);
insert into t1 values (12,34);
