drop database if exists `split_region`;
create database `split_region`;
use `split_region`;
create table test1 (id int primary key, val int);
create table test2 (id int primary key, val int);
insert into test1(id) values (16),(32),(64),(128),(256),(512),(1024),(2048),(4096),(8192),(16384);
insert into test1(id) values (-2147483648),(-1),(0),(2147483647);
