drop database if exists `sync_collation2`;
create database `sync_collation2` character set utf8mb4 collate utf8mb4_general_ci;
use `sync_collation2`;
create table t1 (id int, name varchar(20), primary key(`id`)) character set utf8mb4 collate utf8mb4_general_ci;
insert into t1 (id, name) values (1, 'Aa'), (2, 'aA');
create table t2 (id int, name varchar(20) character set utf8mb4 collate utf8mb4_general_ci, primary key(`id`)) character set latin1 collate latin1_bin;
insert into t2 (id, name) values (1, 'Aa'), (2, 'aA');

drop database if exists `test_panic`;
create database `test_panic`;
use `test_panic`;
create table t1 (
    id int PRIMARY KEY,
    name varchar(20) COLLATE utf8mb4_0900_as_cs
);
