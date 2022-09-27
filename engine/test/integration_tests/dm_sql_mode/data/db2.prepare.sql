set @@session.sql_mode='ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,REAL_AS_FLOAT';
-- NO_AUTO_CREATE_USER set failed in mysql8.0

drop database if exists `sql_mode`;
create database `sql_mode` character set utf8mb4 collate utf8mb4_general_ci;
use `sql_mode`;
CREATE TABLE `t_2` (
   `id` int NOT NULL AUTO_INCREMENT,
   `name` varchar(60),
   `num` int,
   `dt` datetime,
   PRIMARY KEY (id)
) character set utf8mb4 collate utf8mb4_general_ci;

-- test sql_mode PIPES_AS_CONCAT
set @@session.sql_mode=concat(@@session.sql_mode, ',PIPES_AS_CONCAT');
insert into t_2(name) values('pipes'||'as'||'concat');

-- test sql_mode ANSI_QUOTES
insert into t_2(name) values("a");

-- test sql_mode IGNORE_SPACE
set @@session.sql_mode=concat(@@session.sql_mode, ',IGNORE_SPACE');
insert into t_2(name) values(concat ('ignore', 'space'));

-- test sql_mode NO_AUTO_VALUE_ON_ZERO
set @@session.sql_mode=concat(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO');
insert into t_2(id, name) values (10, 'a');
insert into t_2(id, name) values (0, 'b');

-- test sql_mode NO_BACKSLASH_ESCAPES
set @@session.sql_mode=concat(@@session.sql_mode, ',NO_BACKSLASH_ESCAPES');
insert into t_2(name) values ('\\a');

-- test GBK charset BTW
create table t_4 (id int, name varchar(20), primary key(`id`)) character set gbk;
insert into t_4 (id, name) values (0, '你好');
insert into t_4 (id, name) values (1, '你好Aa'), (2, '你好aA');

create table t_6 (id int, name varchar(20) character set gbk, primary key(`id`)) character set utf8mb4 collate utf8mb4_bin;
insert into t_6 (id, name) values (0, '你好');
insert into t_6 (id, name) values (1, '你好Aa'), (2, '你好aA');
