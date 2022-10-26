drop database if exists `savepoint`;
create database `savepoint`;
use `savepoint`;

create table t(id int key, a int, index idx(a));

-- Test savepoint in optimistic transaction.

-- Check savepoint with same name will overwrite the old
begin optimistic;
savepoint s1;
insert into t values (1, 1);
savepoint s1;
insert into t values (2, 2);
rollback to s1;
commit;
select * from t order by id;

--  Check rollback to savepoint will delete the later savepoint.
delete from t;
begin optimistic;
insert into t values (1, 1);
savepoint s1;
insert into t values (2, 2);
savepoint s2;
insert into t values (3, 3);
savepoint s3;
rollback to s2;
commit;

--  Check rollback to savepoint will rollback insert.
delete from t;
begin optimistic;
insert into t values (1, 1), (2, 2);
savepoint s1;
insert into t values (3, 3);
rollback to s1;
insert into t values (3, 5);
commit;

--  Check rollback to savepoint will rollback insert onduplicate update.
delete from t;
insert into t values (1, 1);
begin optimistic;
insert into t values (2, 2);
savepoint s1;
insert into t values (1, 1), (2, 2), (3, 3) on duplicate key update a=a+1;
rollback to s1;
commit;
insert into t values (3, 3);

--  Check rollback to savepoint will rollback update.
delete from t;
begin optimistic;
insert into t values (1, 1), (2, 2);
savepoint s1;
update t set a=a+1 where id = 1;
rollback to s1;
update t set a=a+1 where id = 2;
commit;
update t set a=a+1 where id in (1, 2);

--  Check rollback to savepoint will rollback update.
delete from t;
insert into t values (1, 1), (2, 2);
begin optimistic;
insert into t values (3, 3);
update t set a=a+1 where id in (1, 3);
savepoint s1;
update t set a=a+1 where id in (2, 3);
rollback to s1;
commit;

--  Check rollback to savepoint will rollback delete.
delete from t;
insert into t values (1, 1), (2, 2);
begin optimistic;
insert into t values (3, 3);
savepoint s1;
delete from t where id in (1, 3);
rollback to s1;
commit;

-- Test savepoint in pessimistic transaction.
delete from t;

begin pessimistic;
savepoint s1;
insert into t values (1, 1);
savepoint s1;
insert into t values (2, 2);
rollback to s1;
commit;

--  Check rollback to savepoint will delete the later savepoint.
delete from t;
begin pessimistic;
insert into t values (1, 1);
savepoint s1;
insert into t values (2, 2);
savepoint s2;
insert into t values (3, 3);
savepoint s3;
rollback to s2;
commit;

--  Check rollback to savepoint will rollback insert.
delete from t;
begin pessimistic;
insert into t values (1, 1), (2, 2);
savepoint s1;
insert into t values (3, 3);
rollback to s1;
insert into t values (3, 5);
commit;

--  Check rollback to savepoint will rollback insert onduplicate update and release lock.
delete from t;
insert into t values (1, 1);
begin pessimistic;
insert into t values (2, 2);
savepoint s1;
insert into t values (1, 1), (2, 2), (3, 3) on duplicate key update a=a+1;
rollback to s1;
commit;
insert into t values (3, 3);

--  Check rollback to savepoint will rollback update.
delete from t;
begin pessimistic;
insert into t values (1, 1), (2, 2);
savepoint s1;
update t set a=a+1 where id = 1;
rollback to s1;
update t set a=a+1 where id = 2;
commit;
update t set a=a+1 where id in (1, 2);

--  Check rollback to savepoint will rollback update.
delete from t;
insert into t values (1, 1), (2, 2);
begin pessimistic;
insert into t values (3, 3);
update t set a=a+1 where id in (1, 3);
savepoint s1;
update t set a=a+1 where id in (2, 3);
rollback to s1;
commit;

--  Check rollback to savepoint will rollback delete.
delete from t;
insert into t values (1, 1), (2, 2);
begin pessimistic;
insert into t values (3, 3);
savepoint s1;
delete from t where id in (1, 3);
rollback to s1;
commit;

create table finish_mark (id int PRIMARY KEY);

