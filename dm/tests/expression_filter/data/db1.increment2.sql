use expr_filter;

-- test generated column
insert into t2 (id, should_skip, c) values (1, 0, 1), (2, 1, 2);
-- test change table structure
alter table t2 add column d int;
insert into t2 (id, should_skip, c) values (3, 0, 3), (4, 1, 4);
-- test filter become invalid
alter table t2 drop column c;
insert into t2 (id, should_skip) values (5, 0), (6, 0);
-- test filter become valid again, and the checking column is a generated column
alter table t2 add column c int as (id + 1);
insert into t2 (id, should_skip, d) values (7, 1, 100), (8, 0, 200), (10, 2, 300);
update t2 set should_skip = 0 where id = 10;

-- test a new created table
create table t3 (id int primary key,
    should_skip int,
    ts datetime
);
insert into t3 values (1, 0, '1999-01-01'), (2, 1, '2040-01-01');

create table t4 (id int primary key,
                 should_skip int,
                 a double,
                 b double,
                 c double
);
insert into t4 values (1, 0, 1, 2, 3), (2, 1, 3, 4, 5);

-- test UPDATE
create table t5 (id int primary key,
                 should_skip int,
                 c int
);
insert into t5 values (1, 0, 99), (2, 0, 101), (3, 0, 1);
update t5 set should_skip = 1, c = 110 where id = 1;
update t5 set should_skip = 1, c = 20 where id = 2;
update t5 set should_skip = 1, c = 2 where c = 1;

-- test update-old-value-expr and update-new-value-expr is AND logic
insert into t5 values (4, 1, 1); -- check this `should_skip = 1` row must be updated to `should_skip = 0` in TiDB
update t5 set should_skip = 0, c = 3 where c = 1;

insert into t6 (id, name, msg) values (1, 'Müller', 'Müller'), (2, 'X Æ A-12', 'X Æ A-12');
alter table t6 add column name2 varchar(20) character set latin1 default 'Müller';

-- test https://github.com/pingcap/tiflow/issues/7774
UPDATE t7 SET s = s + 1 WHERE a = 1;

-- trigger a flush
alter table t5 add column dummy int;
