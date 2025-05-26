#!/bin/sh

set -ex

cd "$(dirname "$0")"
OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

for port in 4000 4001; do
  mysql -uroot -h 127.0.0.1 -P $port -e "create database if not exists fix_sql_test;"
  mysql -uroot -h 127.0.0.1 -P $port -e "create table fix_sql_test.fix_table(id int, col varchar(255));"
done

# Insert different values into two data sources
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into fix_sql_test.fix_table values (1, '\b\n\\\\a\\'\"\`');"
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into fix_sql_test.fix_table values (1, '\t\nb\\\\\\'');"

echo "check result should be failed"
sync_diff_inspector --config=./config.toml > $OUT_DIR/fix_sql_test.output || true
check_contains "check failed!!!" $OUT_DIR/sync_diff.log

echo "applying fix SQL"
cat $OUT_DIR/fix-on-tidb/*.sql | mysql -uroot -h127.0.0.1 -P 4000
rm -rf $OUT_DIR/*

echo "check result should be pass after applying fix SQL"
sync_diff_inspector --config=./config.toml > $OUT_DIR/fix_sql_test.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*
