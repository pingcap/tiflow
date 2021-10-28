drop database if exists `sharding_seq_opt`;
create database `sharding_seq_opt`;
use `sharding_seq_opt`;
create table t2 (id bigint primary key, c1 varchar(20), c2 varchar(20)) CHARSET=latin1 COLLATE=latin1_bin;
create table t3 (id bigint primary key, c1 varchar(20), c2 varchar(20)) CHARSET=latin1 COLLATE=latin1_bin;
create table t4 (id bigint primary key, c1 varchar(20), c2 varchar(20)) CHARSET=latin1 COLLATE=latin1_bin;
insert into t2 (id, c1, c2) values (300001, 'five', 'fifth'), (300002, 'six', 'sixth');
insert into t3 (id, c1, c2) values (400001, 'seven', 'seventh'), (400002, 'eight', 'eighth');
insert into t4 (id, c1, c2) values (500001, 'nine', 'nineth'), (500002, 'ten', 'tenth');
