drop database if exists `fake_rotate_event`;

create database `fake_rotate_event`;

use `fake_rotate_event`;

create table t1 (c1 int, c2 int, c3 int, primary key(c1));

insert into
    t1
values
    (1, 1, 1);

insert into
    t1
values
    (2, 2, 2);