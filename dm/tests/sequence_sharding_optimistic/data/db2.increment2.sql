use `sharding_seq_opt`;

-- at this point:
--      t2(id, c2, c3)
--      t3(id, c2, c3)
--      t4(id, c2, c3)
insert into t2 (id, c2, c3) values (300010, '320010', 330010);
insert into t3 (id, c2, c3) values (400010, '420010', 430010);
insert into t4 (id, c2, c3) values (500010, '520010', 530010);
