drop database if exists `online_ddl`;
create database `online_ddl`;
use `online_ddl`;
create table check_pt_t (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into check_pt_t (uid, name) values (20001, 'José Arcadio Buendía'), (20002, 'Úrsula Iguarán'), (20003, 'José Arcadio');
create table _check_pt_t_new like check_pt_t;