use sharding_seq;
delete from t3 where uid = 400002;
insert into t4 (uid,name) values(500005,'`.`'),(500006,'exit');
alter table t2 add column c int;
alter table t2 add index c(c);
update t2 set c = 100;
alter table t2 add column d int;
alter table t2 add index d(d);
alter table t2 add column e int, add index e(e);
update t2 set d = 200;
alter table t3 add column c int;
alter table t3 add index c(c);
update t3 set c = 100;
alter table t3 add column d int;
alter table t3 add index d(d);
alter table t3 add column e int, add index e(e);
update t3 set d = 200;
alter table t4 add column c int;
alter table t4 add index c(c);
update t4 set c = 100;
alter table t4 add column d int;
alter table t4 add index d(d);
alter table t4 add column e int, add index e(e);
update t4 set d = 200;
update t4 set uid=uid+100000;
insert into t2 (uid,name,c) values(300003,'nvWgBf',73),(300004,'nD1000',4029);
insert into t3 (uid,name,c) values(400004,'1000',1000);
