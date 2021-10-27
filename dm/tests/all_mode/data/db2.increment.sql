use all_mode;
delete from t2 where name = 'Sansa';

-- test sql_mode=NO_AUTO_VALUE_ON_ZERO
insert into t2 (id, name) values (0,'haha')