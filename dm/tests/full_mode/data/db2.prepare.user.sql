drop user if exists 'dm_full';
drop role if exists 'test-8.0';
flush privileges;
create user 'dm_full'@'%' identified by '123456';
grant all privileges on *.* to 'dm_full'@'%';
revoke replication slave, replication client, super on *.* from 'dm_full'@'%';
revoke create temporary tables, lock tables, create routine, alter routine, event, create tablespace, file, shutdown, execute, process, index on *.* from 'dm_full'@'%'; # privileges not supported by TiDB
create role 'test-8.0';
grant 'test-8.0' to 'dm_full';
flush privileges;
