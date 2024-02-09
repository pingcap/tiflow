drop database if exists `ddl_manager`;
create database `ddl_manager`;

drop database if exists `ddl_manager_test1`;
create database `ddl_manager_test1`;

drop database if exists `ddl_manager_test2`;
create database `ddl_manager_test2`;

drop database if exists `ddl_manager_test3`;
create database `ddl_manager_test3`;

use `ddl_manager`;

create table t1 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);
INSERT INTO t1 (val, col0) VALUES (1, 1);
INSERT INTO t1 (val, col0) VALUES (2, 2);
INSERT INTO t1 (val, col0) VALUES (3, 3);
INSERT INTO t1 (val, col0) VALUES (4, 4);
INSERT INTO t1 (val, col0) VALUES (5, 5);


CREATE TABLE t2 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);
INSERT INTO t2 (val, col0) VALUES (1, 1);
INSERT INTO t2 (val, col0) VALUES (2, 2);
INSERT INTO t2 (val, col0) VALUES (3, 3);
INSERT INTO t2 (val, col0) VALUES (4, 4);
INSERT INTO t2 (val, col0) VALUES (5, 5);

drop table  t2;

create table t3 (
                    a int, primary key (a)
) partition by hash(a) partitions 5;
insert into t3 values (1),(2),(3),(4),(5),(6);
insert into t3 values (7),(8),(9);
alter table t3 truncate partition p3;

create table t4 (a int primary key) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
insert into t4 values (1),(2),(3),(4),(5),(6);
insert into t4 values (7),(8),(9);
insert into t4 values (11),(12),(20);
alter table t4 add partition (partition p3 values less than (30), partition p4 values less than (40));

CREATE TABLE t5 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

CREATE TABLE t6 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

DROP TABLE t5;

drop database if exists `ddl_manager_test2`;

CREATE TABLE t7 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

DROP TABLE t6;

CREATE TABLE t8 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

DROP TABLE t7;

CREATE TABLE t9 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

DROP TABLE t8;

CREATE TABLE t10 (
                     id INT AUTO_INCREMENT PRIMARY KEY,
                     val INT DEFAULT 0,
                     col0 INT NOT NULL
);

DROP TABLE t9;


drop database if exists `cross_db_1`;
create database `cross_db_1`;

drop database if exists `cross_db_2`;
create database `cross_db_2`;

use `cross_db_1`;

CREATE TABLE t1 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

RENAME TABLE `cross_db_1`.`t1` TO `cross_db_2`.`t1`;

CREATE TABLE `cross_db_1`.`t2` like `cross_db_2`.`t1`;


CREATE TABLE ddl_manager.finish_mark (
                     id INT AUTO_INCREMENT PRIMARY KEY,
                     val INT DEFAULT 0,
                     col0 INT NOT NULL
);


