drop user if exists 'dm_incremental';
flush privileges;
create user 'dm_incremental'@'%' identified by '123456';
grant all privileges on *.* to 'dm_incremental'@'%';
revoke select, reload on *.* from 'dm_incremental'@'%';
revoke create temporary tables, lock tables, create routine, alter routine, event, create tablespace, file, shutdown, execute, process, index on *.* from 'dm_incremental'@'%'; # privileges not supported by TiDB
flush privileges;
