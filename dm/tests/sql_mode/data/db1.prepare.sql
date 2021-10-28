set @@session.SQL_MODE='';

drop database if exists `sql_mode`;
create database `sql_mode`;
use `sql_mode`;
CREATE TABLE `t_1` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(60),
  `num` int,
  `dt` datetime,
  PRIMARY KEY (id)
);

-- test sql_mode PIPES_AS_CONCAT
insert into t_1(num) values('pipes'||'as'||'concat');

-- test sql_mode ANSI_QUOTES
insert into t_1(name) values("a");

-- test sql_mode IGNORE_SPACE
create table count (id int not null, primary key(id));

-- test sql_mode NO_AUTO_VALUE_ON_ZERO
insert into t_1(id, name) values (10, 'a');
insert into t_1(id, name) values (0, 'b');
insert into t_1(id, name) values (0, 'c');

-- test sql_mode NO_BACKSLASH_ESCAPES
insert into t_1(name) values ('\\a');

-- test sql_mode STRICT_TRANS_TABLES && STRICT_ALL_TABLES && NO_ZERO_IN_DATE && NO_ZERO_DATE && ALLOW_INVALID_DATES
insert into t_1(dt) values('0000-06-00');
insert into t_1(dt) values('0000-00-01');
insert into t_1(dt) values('0000-06-01');
insert into t_1(dt) values('0000-00-00');

-- test sql_mode ERROR_FOR_DIVISION_BY_ZERO
insert into t_1(num) values(4/0);

-- test sql_mode NO_AUTO_CREATE_USER
drop user if exists 'no_auto_create_user';
grant select on *.* to 'no_auto_create_user';

-- test different timezone
create table if not exists `sql_mode`.`timezone` (`id` int, `a` timestamp, PRIMARY KEY (id));
set @@session.time_zone = "Asia/Shanghai";
insert into `sql_mode`.`timezone`(`id`, `a`) values (1, '1990-04-15 01:30:12');
set @@session.time_zone = "America/Phoenix";
insert into `sql_mode`.`timezone`(`id`, `a`) values (4, '1990-04-15 01:30:12');
