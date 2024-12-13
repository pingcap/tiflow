use test;
set GLOBAL tidb_enable_batch_dml='ON';
set SESSION tidb_batch_insert='ON';
set SESSION tidb_dml_batch_size=1;
BEGIN;
LOAD DATA LOCAL INFILE '/xxx/tiflow/tests/integration_tests/transaction/data/t1.csv' INTO TABLE test.t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' (val1,val2);
COMMIT;
create table finish_mark
(
    id int PRIMARY KEY
);