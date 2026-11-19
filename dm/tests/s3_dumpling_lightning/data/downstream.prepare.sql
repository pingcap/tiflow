create database `s3_dumpling_lightning`;
use `s3_dumpling_lightning`;
-- Keep the same columns as the upstream tables, but reverse their physical
-- order to verify that Syncer uses the Dumpling schema for row events.
create table t (name varchar(20), id int, primary key(`id`));
