use `sql_mode`;

SET @@session.SQL_MODE="";
set @@session.time_zone = "America/Phoenix";

insert into
    `sql_mode`.`timezone`(`id`, `a`)
values
    (5, '2001-04-15 02:30:12');

insert into
    `sql_mode`.`timezone`(`id`, `a`)
values
    (6, '2001-04-15 03:30:12');
