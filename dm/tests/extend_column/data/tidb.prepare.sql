drop database if exists `extend_column`;
create database `extend_column`;
use `extend_column`;
create table t (c1 int, c2 int, c3 int, c_table varchar(64), c_schema varchar(64), c_source varchar(64), primary key(c1, c_table, c_schema, c_source));
create table y (c1 int, c2 int, c3 int, gc3 int GENERATED ALWAYS AS (c1 + 1) VIRTUAL, c_table varchar(64), primary key(c1, c_table));
