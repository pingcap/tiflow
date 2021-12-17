drop user if exists 'dm_full';
flush privileges;
create user 'dm_full'@'%' identified by '123456';
grant all privileges on *.* to 'dm_full'@'%';
revoke replication slave, replication client on *.* from 'dm_full'@'%';
revoke create temporary tables, lock tables, create routine, alter routine, event, create tablespace, file, shutdown, execute, process, index on *.* from 'dm_full'@'%'; # privileges not supported by TiDB
flush privileges;
