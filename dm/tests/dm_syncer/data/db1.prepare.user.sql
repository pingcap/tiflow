drop user if exists 'dm_syncer';
flush privileges;
create user 'dm_syncer'@'%' identified by '';
grant all privileges on *.* to 'dm_syncer'@'%';
revoke select, reload on *.* from 'dm_syncer'@'%';
revoke create temporary tables, lock tables, create routine, alter routine, event, create tablespace, file, shutdown, execute, process, index on *.* from 'dm_syncer'@'%'; # privileges not supported by TiDB
flush privileges;
