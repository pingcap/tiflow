use foreign_key;

set @@foreign_key_checks=1;
insert into t1 values (7),(8);
insert into t2 values (7),(8);
delete from t1 where id=7;


