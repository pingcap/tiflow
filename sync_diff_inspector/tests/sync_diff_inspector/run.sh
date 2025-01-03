#!/bin/sh

set -ex

cd "$(dirname "$0")"

# check mysql status
check_db_status "${MYSQL_HOST}" "${MYSQL_PORT}" mysql "."

BASE_DIR=/tmp/tidb_tools_test/sync_diff_inspector
OUT_DIR=$BASE_DIR/output


mkdir -p $OUT_DIR || true

echo "use importer to generate test data"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create database if not exists diff_test"
# TODO: run `importer -t "create table diff_test.test(\`table\` int, b varchar(10), c float, d datetime, primary key(a));" -c 10 -n 10000 -P 4000 -h 127.0.0.1 -D diff_test -b 1000`
# will exit with parser error, need to fix it in importer later, just change column name by mysql client now
importer -t "create table diff_test.test(a int, aa int, b varchar(10), c float, d datetime, primary key(a), key(aa));" -c 10 -n 10000 -P 4000 -h 127.0.0.1 -D diff_test -b 1000
mysql -uroot -h 127.0.0.1 -P 4000 -e "alter table diff_test.test change column a \`table\` int"

echo "dump data and then load to tidb and mysql"
dumpling --host 127.0.0.1 --port 4000 --user root -o $BASE_DIR/dump_diff -B diff_test -T "diff_test.test"
loader -h 127.0.0.1 -P 4001 -u root -d $BASE_DIR/dump_diff
mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u root -e "create database if not exists tidb_loader"
loader -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u root -d $BASE_DIR/dump_diff
mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u root -e "select * from diff_test.test limit 10;"

echo "use sync_diff_inspector to compare data"
# sync diff tidb-tidb
sync_diff_inspector --config=./config_base_tidb.toml > $OUT_DIR/diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log

echo "analyze table, and will use tidb's statistical information to split chunks"
check_contains "split range by random" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*
mysql -uroot -h 127.0.0.1 -P 4000 -e "analyze table diff_test.test"
# run the explain SQL to load the stats after analyze
mysql -uroot -h 127.0.0.1 -P 4000 -e "explain select * from diff_test.test where aa > 1"
mysql -uroot -h 127.0.0.1 -P 4000 -e "explain select * from diff_test.test where \`table\` > 1"
mysql -uroot -h 127.0.0.1 -P 4000 -e "show stats_buckets"
sync_diff_inspector --config=./config_base_tidb.toml > $OUT_DIR/diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
check_not_contains "split range by random" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "test 'exclude-tables' config"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table if not exists diff_test.should_not_compare (id int)"
sync_diff_inspector --config=./config_base_tidb.toml > $OUT_DIR/diff.log
# doesn't contain the table's result in check report
check_not_contains "[table=should_not_compare]" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

# sync diff tidb-mysql
sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base_mysql.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config_base_mysql_.toml
sync_diff_inspector --config=./config_base_mysql_.toml #> $OUT_DIR/diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

for script in ./*/run.sh; do
    test_name="$(basename "$(dirname "$script")")"
    echo "---------------------------------------"
    echo "Running test $script..."
    echo "---------------------------------------"
    sh "$script"
done
