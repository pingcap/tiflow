drop database if exists `online_ddl`;
create database `online_ddl`;
use `online_ddl`;
create table check_gho_t (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into check_gho_t (uid, name) values (10001, 'Gabriel García Márquez'), (10002, 'Cien años de soledad');
create table _check_gho_t_gho like check_gho_t;