#!/bin/sh

set -ex

cd "$(dirname "$0")"
OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
FIX_DIR=/tmp/tidb_tools_test/sync_diff_inspector/fixsql
rm -rf $OUT_DIR
rm -rf $FIX_DIR
mkdir -p $OUT_DIR
mkdir -p $FIX_DIR

mysql -uroot -h 127.0.0.1 -P 4000 -e "create database if not exists collation_test;"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create database if not exists collation_test;"

echo "Test1: collation with different ci"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table collation_test.t (a varchar(16) collate utf8mb4_general_ci, b int, primary key(a, b));"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into collation_test.t values ('a', 2), ('B', 2), ('c', 4), ('D', 3);"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create table collation_test.t (a varchar(16) collate utf8mb4_bin, b int, primary key(a, b));"
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into collation_test.t values ('a', 1), ('B', 2), ('c', 4), ('D', 33);"

echo "check should return two different rows"
sync_diff_inspector --config=./config1.toml > $OUT_DIR/expression_diff.output || true
check_contains "+2/-2" $OUT_DIR/summary.txt
rm -rf $OUT_DIR/*

echo "Test2: both collations with ci"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table collation_test.t2 (a varchar(16) collate utf8mb4_unicode_ci, b int, primary key(a, b));"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into collation_test.t2 values ('a', 2), ('B', 2), ('c', 4), ('æ', 3);"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create table collation_test.t2 (a varchar(16) collate utf8mb4_general_ci, b int, primary key(a, b));"
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into collation_test.t2 values ('a', 1), ('B', 3), ('c', 4), ('æ', 4);"

echo "check should return three different rows"
sync_diff_inspector --config=./config2.toml > $OUT_DIR/expression_diff.output || true
check_contains "+3/-3" $OUT_DIR/summary.txt
rm -rf $OUT_DIR/*