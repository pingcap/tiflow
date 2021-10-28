drop database if exists `sharding_seq_opt`;
create database `sharding_seq_opt`;
use `sharding_seq_opt`;
create table t1 (id bigint primary key, c1 varchar(20), c2 varchar(20)) CHARSET=latin1 COLLATE=latin1_bin;
create table t2 (id bigint primary key, c1 varchar(20), c2 varchar(20)) CHARSET=latin1 COLLATE=latin1_bin;
insert into t1 (id, c1, c2) values (100001, 'one', 'first'), (100002, 'two', 'second');
insert into t1 (id, c1, c2) values (200001, 'three', 'third'), (200002, 'four', 'fourth');
