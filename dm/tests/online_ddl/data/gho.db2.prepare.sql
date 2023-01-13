drop database if exists `online_ddl`;
create database `online_ddl`;
use `online_ddl`;
create table gho_t2 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin auto_increment=300000;
create table gho_t3 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin auto_increment=400000;
insert into gho_t2 (uid, name, info) values (40000, 'Remedios Moscote', '{}'), (40001, 'Amaranta', '{"age": 0}');
insert into gho_t3 (uid, name, info) values (30001, 'Aureliano José', '{}'), (30002, 'Santa Sofía de la Piedad', '{}'), (30003, '17 Aurelianos', NULL);
