drop database if exists `database-placeholder`;
create database `database-placeholder`;
use `database-placeholder`;
create table tb (id int auto_increment, name varchar(20), primary key (`id`));
insert into tb (name) values ('Arya'), ('Bran'), ('Sansa');
