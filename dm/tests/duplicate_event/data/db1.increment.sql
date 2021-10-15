use `dup_event`;

begin;
insert into t1 (id, name) values (11, 'test11');
insert into t1 (id, name) values (12, 'test12');
insert into t1 (id, name) values (13, 'test13');
insert into t1 (id, name) values (14, 'test14');
insert into t1 (id, name) values (15, 'test15');
commit;

begin;
insert into t1 (id, name) values (21, 'test21');
insert into t1 (id, name) values (22, 'test22');
insert into t1 (id, name) values (23, 'test23');
insert into t1 (id, name) values (24, 'test24');
insert into t1 (id, name) values (25, 'test25');
commit;

begin;
insert into t1 (id, name) values (31, 'test31');
insert into t1 (id, name) values (32, 'test32');
insert into t1 (id, name) values (33, 'test33');
insert into t1 (id, name) values (34, 'test34');
insert into t1 (id, name) values (35, 'test35');
commit;