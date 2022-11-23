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
