drop database if exists `all_mode`;
create database `all_mode`;
use `all_mode`;
create table t1 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    dt datetime,
    ts timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id));
-- test ANSI_QUOTES works with quote in string
insert into t1 (id, name, dt, ts) values (1, 'ar"ya', now(), now()), (2, 'catelyn', '2021-05-11 10:01:05', '2021-05-11 10:01:05');

-- test sql_mode=NO_AUTO_VALUE_ON_ZERO
insert into t1 (id, name) values (0, 'lalala');

-- test block-allow-list
drop database if exists `ignore_db`;
create database `ignore_db`;
use `ignore_db`;
create table `ignore_table`(id int);