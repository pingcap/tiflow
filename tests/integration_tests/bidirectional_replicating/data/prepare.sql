drop database if exists `bidirectional_replication`;
create database `bidirectional_replication`;
use `bidirectional_replication`;

create table t1 (id int primary key);


insert into t1 values (1);
