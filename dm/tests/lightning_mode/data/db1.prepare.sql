drop database if exists `lightning_mode`;
create database `lightning_mode`;
use `lightning_mode`;
create table t1 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    PRIMARY KEY (id));
-- test ANSI_QUOTES works with quote in string
insert into t1 (id, name) values (1, 'ar"ya'), (2, 'catelyn');

-- test sql_mode=NO_AUTO_VALUE_ON_ZERO
insert into t1 (id, name) values (0, 'lalala');

-- test block-allow-list
drop database if exists `ignore_db`;
create database `ignore_db`;
use `ignore_db`;
create table `ignore_table`(id int);