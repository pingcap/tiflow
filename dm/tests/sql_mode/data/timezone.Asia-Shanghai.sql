use `sql_mode`;

SET @@session.SQL_MODE="";
set @@session.time_zone = "Asia/Shanghai";

insert into
    `sql_mode`.`timezone`(`id`, `a`)
values
    (2, '2001-04-15 02:30:12');

insert into
    `sql_mode`.`timezone`(`id`, `a`)
values
    (3, '2001-04-15 03:30:12');
