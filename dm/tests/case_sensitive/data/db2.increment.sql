use lower_db;
delete from Upper_Table where name = 'Sansa';

-- test sql_mode=NO_AUTO_VALUE_ON_ZERO
insert into Upper_Table (id, name) values (0,'haha');

use `lower_db_ignore`;
insert into `Upper_Table` (id) values (1);