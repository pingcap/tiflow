drop database if exists `metrics`;

create database `metrics`;

use `metrics`;

create table t2 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    ts timestamp,
    PRIMARY KEY (id)
);

insert into
    t2 (name, ts)
values
    ('Arya', now()),
    ('Bran', '2021-05-11 10:01:05'),
    ('Sansa', NULL);
