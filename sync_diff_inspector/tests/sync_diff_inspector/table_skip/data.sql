create database if not exists skip_test;
create table skip_test.t0 (a int, b int, primary key(a));
create table skip_test.t1 (a int, b int, primary key(a));
insert into skip_test.t0 values (1,1);
insert into skip_test.t1 values (2,2);