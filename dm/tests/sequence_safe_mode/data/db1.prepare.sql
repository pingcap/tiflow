drop database if exists `sequence_safe_mode_test`;
create database `sequence_safe_mode_test`;
use `sequence_safe_mode_test`;
create table t1 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
create table t2 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
insert into t1 (uid, name) values (10001, 'Gabriel García Márquez'), (10002, 'Cien años de soledad');
insert into t2 (uid, name) values (20001, 'José Arcadio Buendía'), (20002, 'Úrsula Iguarán'), (20003, 'José Arcadio');
