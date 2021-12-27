drop database if exists `online_ddl`;
create database `online_ddl`;
use `online_ddl`;
create table check_gho_t (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
create table check_pt_t (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into check_gho_t (uid, name) values (10001, 'Gabriel García Márquez'), (10002, 'Cien años de soledad');
insert into check_pt_t (uid, name) values (20001, 'José Arcadio Buendía'), (20002, 'Úrsula Iguarán'), (20003, 'José Arcadio');
create table _check_gho_t_gho like check_gho_t;