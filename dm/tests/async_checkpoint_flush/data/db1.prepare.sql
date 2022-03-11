drop database if exists `async_checkpoint_flush`;
reset master;
create database `async_checkpoint_flush`;
use `async_checkpoint_flush`;
create table t1 (id int, primary key(`id`));
