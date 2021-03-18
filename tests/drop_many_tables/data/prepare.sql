drop database if exists `drop_tables`;
create database `drop_tables`;
use `drop_tables`;

create table a (id int primary key clustered );
create table b (id int primary key clustered );
drop table a,b;

create table a (id varchar(16) primary key clustered );
create table b (id varchar(16) primary key clustered );

insert into a value ('aa');
insert into b value ('bb');

create table c (id int primary key clustered );