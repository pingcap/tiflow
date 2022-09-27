set @@session.sql_mode='ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,REAL_AS_FLOAT';
-- NO_AUTO_CREATE_USER set failed in mysql8.0

use sql_mode;

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
insert into t_2(id, name) values (20, 'a');
replace into t_2(id, name) values (0, 'c');

-- test sql_mode NO_BACKSLASH_ESCAPES
set @@session.sql_mode=concat(@@session.sql_mode, ',NO_BACKSLASH_ESCAPES');
insert into t_2(name) values ('\\a');

-- test GBK charset BTW
insert into t_4 (id, name) values (3, '你好aa3'), (4, '你好AA4');
update t_4 set name = '你好Aa5' where id = 1;
delete from t_4 where id = 2;

create table t_8 (id int, name varchar(20), primary key(`id`)) character set gbk;
insert into t_8 (id, name) values (1, '你好Aa'), (2, '你好aA');
insert into t_8 (id, name) values (3, '你好aa3'), (4, '你好AA4');
update t_8 set name = '你好Aa5' where id = 1;
delete from t_8 where id = 2;

create table t_10 (id int, name varchar(20) character set gbk, primary key(`id`)) character set utf8mb4 collate utf8mb4_bin;
insert into t_10 (id, name) values (1, '你好Aa'), (2, '你好aA');
insert into t_10 (id, name) values (3, '你好aa3'), (4, '你好AA4');
update t_10 set name = '你好Aa5' where id = 1;
delete from t_10 where id = 2;
