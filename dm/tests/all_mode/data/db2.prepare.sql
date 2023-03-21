drop database if exists `all_mode`;
create database `all_mode`;
use `all_mode`;
create table t2 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    ts timestamp,
    PRIMARY KEY (id));
insert into t2 (name, ts) values ('Arya', now()), ('Bran', '2021-05-11 10:01:05'), ('Sansa', NULL);

-- test block-allow-list
drop database if exists `ignore_db`;
create database `ignore_db`;
use `ignore_db`;
create table `ignore_table`(id int);

use `all_mode`;
CREATE TABLE t3 (
    id INT PRIMARY KEY,
    j JSON,
    KEY j_index ((cast(json_extract(j,_utf8mb4'$[*]') as signed array)), id)
);
INSERT INTO t3 VALUES (1, '[1,2,3]'), (2, '[2,3,4]'), (3, '[3,4,5]');
