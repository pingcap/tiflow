use database-placeholder;
-- add an empty column to flush syncer checkpoint
alter table tb add column info varchar(10);
delete from tb where name = 'Sansa';
