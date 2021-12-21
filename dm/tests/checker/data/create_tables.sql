drop database if exists `checker`;
create database `checker`;
use `checker`;

delimiter $$
create procedure sp_create_table()
begin
    declare i int(4) unsigned zerofill;
    declare sqlstr varchar(2048);
    set i = 0;
    set sqlstr = "";
    while i < 10000 do
        set sqlstr = concat(
            "create table checker_",
            i,
            "(
              `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
              `region` varchar(4) NOT NULL,
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            "
        );
        set @sqlstr = sqlstr;
        prepare stmt from @sqlstr;
        execute stmt;
        set i = i + 1;
    end while;
end$$
delimiter ;
call sp_create_table();
drop procedure sp_create_table;

-- produce batch delete statement
-- select concat('drop table ', group_concat(table_name), ';') as statement from infomation_schema.tables where table_schema = 'xxx_db' and table_name like 'yyy_tab_00000%';
-- mysql -uroot -h127.0.0.1 -P3306 -p123456 -vv <"dm/tests/checker/data/create_tables.sql"