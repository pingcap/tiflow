use sharding_seq;
update t4 set c = 100;
alter table t4 add column d int;
alter table t4 add index d(d);
alter table t4 add column e int, add index e(e);
update t4 set d = 200;
update t4 set uid=uid+100000;
insert into t2 (uid,name,c) values(300003,'nvWgBf',73),(300004,'nD1000',4029);
insert into t3 (uid,name,c) values(400004,'1000',1000);