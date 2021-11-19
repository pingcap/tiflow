drop database if exists `tracker_ignored_ddl`;
create database `tracker_ignored_ddl`;
use `tracker_ignored_ddl`;

create table t1 (
    `id` int NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`)
);

insert into t1 values (1);