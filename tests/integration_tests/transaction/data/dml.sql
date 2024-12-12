use test;
set GLOBAL tidb_dml_batch_size=1;
set GLOBAL tidb_enable_batch_dml='ON';
set GLOBAL tidb_batch_insert='ON';
BEGIN;
create table start_mark
(
    id int PRIMARY KEY
);
LOAD DATA LOCAL INFILE 'data/t1.csv' INTO TABLE test.t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (id,val);
create table finish_mark
(
    id int PRIMARY KEY
);
COMMIT;
