use foreign_key;

set @@foreign_key_checks=1;
insert into t1 values (7),(8);
insert into t2 values (7),(8);
delete from t1 where id=7;

insert into t3 values (1),(2);
insert into t4 values (1, 1),(2, 2);
delete from t3 where id in (1);
update t3 set id=id+10 where id =2;
