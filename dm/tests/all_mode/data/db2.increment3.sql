use all_mode;
begin;
insert into t2 (id, name) values (10, '10');
insert into t2 (id, name) values (20, '20');
commit;
insert into t2 (id, name) values (30, '30');
