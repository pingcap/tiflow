drop database if exists `Upper_DB`;
create database `Upper_DB`;
use `Upper_DB`;
create table Do_Table (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    dt datetime,
    ts timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id));
-- test ANSI_QUOTES works with quote in string
insert into Do_Table (id, name, dt, ts) values (1, 'ar"ya', now(), now()), (2, 'catelyn', '2021-05-11 10:01:05', '2021-05-11 10:01:05');

create table Do_table_ignore(id int PRIMARY KEY);
insert into Do_table_ignore values (1);

create table lower_table (id int NOT NULL, val varchar(12), PRIMARY KEY (id));
insert into lower_table (id, val) values (0, 'lalala');

-- should be ignored
create table lower_Table1 (id int NOT NULL PRIMARY KEY);

drop database if exists `Upper_DB1`;
create database `Upper_DB1`;
use `Upper_DB1`;
create table Do_Table1 (id int NOT NULL AUTO_INCREMENT, name varchar(20), PRIMARY KEY (id));
insert into Do_Table1 (id, name) values (1, 'test'), (2, 'test2');

drop database IF EXISTS `Upper_Db_IGNORE`;
create database `Upper_Db_IGNORE`;
use `Upper_Db_IGNORE`;
create table `ignore_table`(id int);