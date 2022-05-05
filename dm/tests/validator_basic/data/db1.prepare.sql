drop database if exists `validator_basic`;
create database `validator_basic`;
use `validator_basic`;
create table t1 (id int primary key, name varchar(20), c3 timestamp, c4 datetime, c5 float, c6 double);
insert into t1 values
    (1, 'a', now(), '2022-04-29 15:30:00', 123456789123456789, 123456789123456123456789123456789);
