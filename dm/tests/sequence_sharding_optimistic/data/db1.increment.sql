use `sharding_seq_opt`;

-- at this point:
--      t1(id, c1, c2)
--      t2(id, c1, c2)
insert into t2 (id, c1, c2) values (200003, '210003', '220003');
insert into t1 (id, c1, c2) values (100003, '110003', '120003');

alter table t1 drop column c1;
-- at this point:
--      t1(id,     c2)
--      t2(id, c1, c2)
insert into t1 (id, c2) values (100004, '120004');
insert into t2 (id, c1, c2) values (200004, '210004', '220004');

alter table t2 add column c3 int;
-- at this point:
--      t1(id,     c2)
--      t2(id, c1, c2, c3)
insert into t2 (id, c1, c2, c3) values (200005, '210005', '220005', 230005);
insert into t1 (id, c2) values (100005, '120005');

alter table t2 drop column c1;
-- at this point:
--      t1(id, c2)
--      t2(id, c2, c3)
insert into t1 (id, c2) values (100006, '120006');
insert into t2 (id, c2, c3) values (200006, '220006', 230006);

alter table t1 add column c3 int;
-- at this point:
--      t1(id, c2, c3)
--      t2(id, c2, c3)
insert into t2 (id, c2, c3) values (200007, '220007', 230007);
insert into t1 (id, c2, c3) values (100007, '120007', 130007);
