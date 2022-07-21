-- test the db name that must be quoted are working properly;
use `check-task`;

create table t1(c int primary key);

create table t2(c int primary key, c2 int);

create table t3(c int primary key, c3 int);

create table t4(c int primary key, c4 int);

create table t5(c int primary key, c5 int);

create table t6(c int primary key, c6 int);
