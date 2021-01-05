drop database if exists `partition_table`;
create database `partition_table`;
use `partition_table`;
set @@session.tidb_enable_table_partition = nightly;

create table t (a int, primary key (a)) partition by hash(a) partitions 5;
insert into t values (1),(2),(3),(4),(5),(6);
insert into t values (7),(8),(9);
alter table t truncate partition p3;
update t set a=a+10 where a=2;


create table t1 (a int primary key) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
insert into t1 values (1),(2),(3),(4),(5),(6);
insert into t1 values (7),(8),(9);
insert into t1 values (11),(12),(20);
alter table t1 add partition (partition p3 values less than (30), partition p4 values less than (40));
insert into t1 values (25),(29),(35);
alter table t1 truncate partition p0;
alter table t1 drop partition p1;
insert into t1 values (7),(8),(9);
update t1 set a=a+10 where a=9;

create table t2 (id int, name varchar(10), unique index idx (id)) partition by list  (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
);
insert into t2 values  (1, 'a'),(2,'b'),(3,'c'),(4,'d'),(5,'f');
insert into t2 values  (7, 'g');
insert into t2 values  (null, 'h');
delete from t2 where id < 3;
update t2 set name='x' where id=7;
update t2 set id=9 where id=7;
alter table t2 truncate partition p0;
alter table t2 drop partition p1;
insert into t2 values  (3, 'aa'),(5,'bb'),(6,'cc');

create table t3 (id int, name varchar(10),b int generated always as (length(name)+1) virtual, unique index idx (id,b))
      partition by list  (id*2 + b*b + b*b - b*b*2 - abs(id)) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
);
insert into t3 (id,name) values  (1, 'a'),(2,'b'),(10,'c');
delete from t3;
insert into t3 (id,name) values  (1, 'a'),(3,'c'),(4,'e');
insert into t3 (id,name) values (1, 'd'), (3,'f'),(5,'g') on duplicate key update name='x';
insert ignore into t3 (id,name) values  (1, 'b'), (5,'a'),(null,'y');
insert ignore into t3 (id,name) values  (15, 'a'),(17,'a');
delete from t3;
insert into t3 (id,name) values  (1, 'a'),(2,'b'),(3,'c');
update t3 set name='b' where id=2;
update t3 set name='x' where id in (1,2);
update t3 set id=id*10 where id in (1,2);
alter table t3 truncate partition p0;
alter table t3 drop partition p1;
insert into t3 (id,name) values  (3, 'c'),(5,'g'),(6,'h');


create table t4 (location varchar(10), id int, a int, unique index idx (location,id)) partition by list columns (location,id) (
    	partition p_west  values in (('w', 1),('w', 2),('w', 3),('w', 4)),
    	partition p_east  values in (('e', 5),('e', 6),('e', 7),('e', 8)),
    	partition p_north values in (('n', 9),('n',10),('n',11),('n',12)),
    	partition p_south values in (('s',13),('s',14),('s',15),('s',16))
);
insert into t4 values  ('w', 1, 1),('e', 5, 5),('n', 9, 9);
insert into t4 values  ('w', 1, 1) on duplicate key update a=a+1;
insert into t4 values  ('w', 1, 1) on duplicate key update location='s', id=13;
insert into t4 values  ('w', 2, 2), ('w', 1, 1);
alter table t4 truncate partition p_west;
alter table t4 drop partition p_east;
insert into t4 values  ('w', 2, 2), ('w', 1, 1);
