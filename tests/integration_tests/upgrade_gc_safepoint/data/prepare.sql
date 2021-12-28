drop database if exists `upgrade_gc_safepoint`;
create database `upgrade_gc_safepoint`;
use `upgrade_gc_safepoint`;
create table test1 (id int primary key, val int);
insert into test1(id) values (16),(32),(64),(128),(256),(512),(1024),(2048),(4096),(8192),(16384);
