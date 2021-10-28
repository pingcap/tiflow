use `sharding_seq_opt`;

-- try to update the downstream schema before applying any DML
alter table t4 add column c3 int;
-- at this point:
--      t2(id, c1, c2)
--      t3(id, c1, c2)
--      t4(id, c1, c2, c3)
insert into t4 (id, c1, c2, c3) values (500004, '510004', '520004', 530004);
insert into t2 (id, c1, c2) values (300004, '310004', '320004');
insert into t3 (id, c1, c2) values (400004, '410004', '420004');

alter table t4 drop column c1;
-- at this point:
--      t2(id, c1, c2)
--      t3(id, c1, c2)
--      t4(id,     c2, c3)
insert into t3 (id, c1, c2) values (400005, '410005', '420005');
insert into t4 (id, c2, c3) values (500005, '520005', 530005);
insert into t2 (id, c1, c2) values (300005, '310005', '320005');

alter table t2 drop column c1;
-- at this point:
--      t2(id,     c2)
--      t3(id, c1, c2)
--      t4(id,     c2, c3)
insert into t4 (id, c2, c3) values (500006, '520006', 530006);
insert into t3 (id, c1, c2) values (400006, '410006', '420006');
insert into t2 (id, c2) values (300006, '320006');

alter table t2 add column c3 int;
-- at this point:
--      t2(id,     c2, c3)
--      t3(id, c1, c2)
--      t4(id,     c2, c3)
insert into t2 (id, c2, c3) values (300007, '320007', 330007);
insert into t4 (id, c2, c3) values (500007, '520007', 530007);
insert into t3 (id, c1, c2) values (400007, '410007', '420007');

alter table t3 drop column c1;
-- at this point:
--      t2(id, c2, c3)
--      t3(id, c2)
--      t4(id, c2, c3)
insert into t3 (id, c2) values (400008, '420008');
insert into t2 (id, c2, c3) values (300008, '320008', 330008);
insert into t4 (id, c2, c3) values (500008, '520008', 530008);

alter table t3 add column c3 int;
-- at this point:
--      t2(id, c2, c3)
--      t3(id, c2, c3)
--      t4(id, c2, c3)
insert into t4 (id, c2, c3) values (500009, '520009', 530009);
insert into t3 (id, c2, c3) values (400009, '420009', 430009);
insert into t2 (id, c2, c3) values (300009, '320009', 330009);
