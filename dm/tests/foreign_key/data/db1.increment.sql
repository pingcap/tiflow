use foreign_key;

set @@foreign_key_checks=1;
insert into t1 values (7),(8);
insert into t2 values (7),(8);
delete from t1 where id=7;

insert into t3 values (7),(8);
insert into t4 values (7, 7),(8, 8);
delete from t3 where id in (7);
update t3 set id=id+10 where id =8;
