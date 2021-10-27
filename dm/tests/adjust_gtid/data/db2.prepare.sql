drop database if exists `adjust_gtid`;
create database `adjust_gtid`;
use `adjust_gtid`;
create table t2 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    ts timestamp,
    PRIMARY KEY (id));;
insert into t2 (name, ts) values ('Arya', now()), ('Bran', '2021-05-11 10:01:05'), ('Sansa', NULL);

-- test block-allow-list
drop database if exists `ignore_db`;
create database `ignore_db`;
use `ignore_db`;
create table `ignore_table`(id int);