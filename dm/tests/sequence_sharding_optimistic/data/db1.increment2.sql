use `sharding_seq_opt`;

alter table t2 add column c1 varchar(20);
-- at this point:
--      t1(id,     c2, c3)
--      t2(id, c1, c2, c3)
insert into t2 (id, c1, c2, c3) values (200008, '210008', '220008', 230008);
insert into t1 (id, c2, c3) values (100008, '120008', 130008);

alter table t2 drop column c1;
-- at this point:
--      t1(id, c2, c3)
--      t2(id, c2, c3)
insert into t1 (id, c2, c3) values (100009, '120009', 130009);
insert into t2 (id, c2, c3) values (200009, '220009', 230009);

