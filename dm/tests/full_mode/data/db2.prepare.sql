drop database if exists `full_mode`;
create database `full_mode`;
use `full_mode`;
create table t2 (id int auto_increment, name varchar(20), dt datetime, ts timestamp, primary key (`id`));
insert into t2 (name, dt, ts) values
    ('Arya', now(), now()),
    ('Bran', '2021-05-11 10:09:03', '2021-05-11 10:09:03'),
    ('Sansa', NULL, NULL);
