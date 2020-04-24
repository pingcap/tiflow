drop database if exists `collation_test`;
create database `collation_test`;
use `collation_test`;

CREATE TABLE `t` (
    `a` varchar(20) COLLATE utf8mb4_general_ci NOT NULL,
    PRIMARY KEY (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

insert into `t` (`a`) value ('A');
insert into `t` (`a`) value ('b');
insert into `t` (`a`) value ('C');
insert into `t` (`a`) value ('d');
