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
mysql -uroot -h 127.0.0.1 -P 4002 -e "create database if not exists collation_test;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists collation_test;"

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

echo "Test3: use collation not in new-collations"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table collation_test.t3 (id int PRIMARY KEY, name varchar(20) COLLATE utf8mb4_unicode_520_ci);"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into collation_test.t3 values (1, 'a'), (2, 'b');"
mysql -uroot -h 127.0.0.1 -P 4002 -e "create table collation_test.t3 (id int PRIMARY KEY, name varchar(20) COLLATE utf8mb4_unicode_520_ci);"
mysql -uroot -h 127.0.0.1 -P 4002 -e "insert into collation_test.t3 values (1, 'a'), (2, 'b');"

echo "check should pass"
sync_diff_inspector --config=./config3.toml > $OUT_DIR/expression_diff.output
rm -rf $OUT_DIR/*

echo "Test4: MySQL shards with collation"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table collation_test.shard0 (name varchar(20) COLLATE utf8mb4_general_ci, UNIQUE KEY i(name));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into collation_test.shard0 values ('a'), ('C');"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table collation_test.shard1 (name varchar(20) COLLATE utf8mb4_general_ci, UNIQUE KEY i(name));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into collation_test.shard1 values ('B'), ('d');"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table collation_test.shards (name varchar(20) COLLATE utf8mb4_general_ci, UNIQUE KEY i(name));"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into collation_test.shards values ('a'), ('e'), ('B'), ('C'), ('d');"

echo "check should return one superfluous rows"
sync_diff_inspector --config=./config_shard.toml > $OUT_DIR/expression_diff.output || true
check_contains "+0/-1" $OUT_DIR/summary.txt
rm -rf $OUT_DIR/*