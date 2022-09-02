drop database if exists `lower_db`;
create database `lower_db` collate utf8mb4_general_ci;
use `lower_db`;
create table Upper_Table (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    ts timestamp,
    PRIMARY KEY (id));
insert into Upper_Table (name, ts) values ('Arya', now()), ('Bran', '2021-05-11 10:01:05'), ('Sansa', NULL);

-- if case-insensitive, this should report conflict with Upper_Table
create table upper_table(id int NOT NULL PRIMARY KEY);

-- test block-allow-list
drop database if exists `lower_db_ignore`;
create database `lower_db_ignore`;
use `lower_db_ignore`;
create table `Upper_Table`(id int);
