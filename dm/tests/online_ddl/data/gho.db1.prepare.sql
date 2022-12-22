drop database if exists `online_ddl`;
create database `online_ddl`;
use `online_ddl`;
create table gho_t1 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin auto_increment=100000;
create table gho_t2 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin auto_increment=200000;
insert into gho_t1 (uid, name) values (10001, 'Gabriel García Márquez'), (10002, 'Cien años de soledad');
insert into gho_t2 (uid, name) values (20001, 'José Arcadio Buendía'), (20002, 'Úrsula Iguarán'), (20003, 'José Arcadio');
