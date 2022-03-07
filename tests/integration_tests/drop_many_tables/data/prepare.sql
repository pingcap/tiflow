drop database if exists `drop_tables`;
create database `drop_tables`;
use `drop_tables`;

create table a (id int primary key);
create table b (id int primary key);
drop table a,b;

create table a (id varchar(16) primary key);
create table b (id varchar(16) primary key);

insert into a value ('aa');
insert into b value ('bb');

create table c (id int primary key);