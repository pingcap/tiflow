#!/bin/sh

set -ex

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} < ./data.sql

# tidb
mysql -uroot -h 127.0.0.1 -P 4000 < ./data.sql

sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml

echo "compare tables, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/table_skip_diff.output || true
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "make some tables exist only upstream or downstream"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table skip_test.t2 (a int, b int, primary key(a));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into skip_test.t2 values (3,3);"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table skip_test.t3 (a int, b int, primary key(a));"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into skip_test.t3 values (1,1);"
sync_diff_inspector --config=./config.toml > $OUT_DIR/table_skip_diff.output || true
check_contains "check pass" $OUT_DIR/sync_diff.log
check_contains "Comparing the table data of \`skip_test\`.\`t2\` ...skipped" $OUT_DIR/table_skip_diff.output
check_contains "Comparing the table data of \`skip_test\`.\`t3\` ...skipped" $OUT_DIR/table_skip_diff.output
check_contains "The data of \`skip_test\`.\`t2\` does not exist in downstream database" $OUT_DIR/table_skip_diff.output
check_contains "The data of \`skip_test\`.\`t3\` does not exist in upstream database" $OUT_DIR/table_skip_diff.output
check_contains "|      TABLE       | RESULT  | STRUCTURE EQUALITY | DATA DIFF ROWS | UPCOUNT | DOWNCOUNT |" $OUT_DIR/summary.txt
check_contains "| \`skip_test\`.\`t2\` | skipped | false              | +1/-0          |       1 |         0 |" $OUT_DIR/summary.txt
check_contains "| \`skip_test\`.\`t3\` | skipped | false              | +0/-1          |       0 |         1 |" $OUT_DIR/summary.txt
rm -rf $OUT_DIR/*

echo "make some table data not equal"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into skip_test.t1 values (4,4);"
sync_diff_inspector --config=./config.toml > $OUT_DIR/table_skip_diff.output || true
check_contains "check failed" $OUT_DIR/sync_diff.log
check_contains "| \`skip_test\`.\`t1\` | succeed | true               | +0/-1          |       1 |         2 |" $OUT_DIR/summary.txt
rm -rf $OUT_DIR/*

echo "make some table structure not equal"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table skip_test.t4 (a int, b int, c int,primary key(a));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into skip_test.t4 values (1,1,1);"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table skip_test.t4 (a int, b int, primary key(a));"
sync_diff_inspector --config=./config.toml > $OUT_DIR/table_skip_diff.output || true
check_contains "check failed" $OUT_DIR/sync_diff.log
check_contains "| \`skip_test\`.\`t4\` | succeed | false              | +0/-0          |       0 |         0 |" $OUT_DIR/summary.txt
check_contains "A total of 5 tables have been compared, 1 tables finished, 2 tables failed, 2 tables skipped" $OUT_DIR/table_skip_diff.output
cat $OUT_DIR/summary.txt
rm -rf $OUT_DIR/*

echo "test router case"
sync_diff_inspector --config=./config_router.toml > $OUT_DIR/table_skip_diff.output || true
check_contains "check pass" $OUT_DIR/sync_diff.log
check_contains "| \`skip_test\`.\`t5\` | skipped | false              | +3/-0          |       3 |         0 |" $OUT_DIR/summary.txt
check_contains "The data of \`skip_test\`.\`t5\` does not exist in downstream database" $OUT_DIR/table_skip_diff.output
check_contains "A total of 1 tables have been compared, 0 tables finished, 0 tables failed, 1 tables skipped" $OUT_DIR/table_skip_diff.output
rm -rf $OUT_DIR/*

echo "table_skip test passed"
