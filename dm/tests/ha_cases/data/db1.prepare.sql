drop database if exists `database-placeholder`;
create database `database-placeholder`;
use `database-placeholder`;
create table ta (id int, name varchar(20), primary key(`id`));
insert into ta (id, name) values (1, 'arya'), (2, 'catelyn');
