drop database if exists `safe_mode_test`;
create database `safe_mode_test`;
use `safe_mode_test`;
create table t2 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) AUTO_INCREMENT = 200;
create table t3 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) AUTO_INCREMENT = 300;
insert into t2 (uid, name) values (40000, 'Remedios Moscote'), (40001, 'Amaranta');
insert into t3 (uid, name) values (30001, 'Aureliano José'), (30002, 'Santa Sofía de la Piedad'), (30003, '17 Aurelianos');
