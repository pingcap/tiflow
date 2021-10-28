drop database if exists `metrics`;

create database `metrics`;

use `metrics`;

create table t1 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(20),
    dt datetime,
    ts timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);
