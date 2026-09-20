use dm_syncer;
delete from t2 where name = 'Sansa';

-- MySQL 8.0.21+ writes this to a row-based binlog as
-- CREATE TABLE ... START TRANSACTION followed by row events.
create table t3 as select * from t2;
