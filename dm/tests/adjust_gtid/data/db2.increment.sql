use adjust_gtid;
delete from t2 where name = 'Sansa';

-- test sql_mode=NO_AUTO_VALUE_ON_ZERO
insert into t2 (id, name) values (0,'haha');

create table t3(c int primary key, t time(2));
insert into t3 values(1, '-00:00:00.01');
