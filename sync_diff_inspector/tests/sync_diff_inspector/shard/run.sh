#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

echo "generate data to sharding tables"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists shard_test;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table shard_test.test1(\`table\` int, aa int, b varchar(10), c float, d datetime, primary key(\`table\`));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table shard_test.test2(\`table\` int, aa int, b varchar(10), c float, d datetime, primary key(\`table\`));"

# each table only have part of data
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into shard_test.test1 (\`table\`, aa, b, c, d) SELECT \`table\`, aa, b, c, d FROM diff_test.test WHERE \`table\`%2=0"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into shard_test.test2 (\`table\`, aa, b, c, d) SELECT \`table\`, aa, b, c, d FROM diff_test.test WHERE \`table\`%2=1"

# tidb
mysql -uroot -h 127.0.0.1 -P 4000 -e "create database if not exists shard_test;"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table shard_test.test(\`table\` int, aa int, b varchar(10), c float, d datetime, primary key(\`table\`));"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into shard_test.test (\`table\`, aa, b, c, d) SELECT \`table\`, aa, b, c, d FROM diff_test.test;"

sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml

echo "compare sharding tables with one table in downstream, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "update data in one shard table, and data should not be equal"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "update shard_test.test1 set b = 'abc' limit 1"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.output || true
check_contains "check failed" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "check the router for shard"
# router_test_0.tbl
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists router_test_0;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table router_test_0.tbl (id INT(11), name VARCHAR(25), deptId INT(11));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into router_test_0.tbl values (1,\"hello1\",1);"
# Router_test_0.tbl
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists Router_test_0;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table Router_test_0.tbl (id INT(11), name VARCHAR(25), deptId INT(11));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into Router_test_0.tbl values (1,\"hello1\",1);"
# router_test_0.Tbl
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists router_test_0;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table router_test_0.Tbl (id INT(11), name VARCHAR(25), deptId INT(11));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into router_test_0.Tbl values (1,\"hello1\",1);"
# router_test_1.tbl
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists router_test_1;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table router_test_1.tbl (id INT(11), name VARCHAR(25), deptId INT(11));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into router_test_1.tbl values (1,\"hello1\",1);"
# Router_test_1.tbl
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists Router_test_1;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table Router_test_1.tbl (id INT(11), name VARCHAR(25), deptId INT(11));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into Router_test_1.tbl values (1,\"hello1\",1);"
# router_test_1.Tbl
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists router_test_1;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table router_test_1.Tbl (id INT(11), name VARCHAR(25), deptId INT(11));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into router_test_1.Tbl values (1,\"hello1\",1);"
# Router_test_1.Tbl
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database if not exists Router_test_1;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table Router_test_1.Tbl (id INT(11), name VARCHAR(25), deptId INT(11));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into Router_test_1.Tbl values (1,\"hello1\",1);"

echo "test router 1: normal rule"
sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_router_1.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml
sync_diff_inspector --config=./config.toml -L debug > $OUT_DIR/shard_diff.output || true
check_contains "as CHECKSUM FROM \`router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`Router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`router_test_0\`.\`Tbl\`" $OUT_DIR/sync_diff.log
#check_not_contains "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains_count "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log 1
check_not_contains "as CHECKSUM FROM \`Router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`Router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "test router 2: only schema rule"
sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_router_2.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml
sync_diff_inspector --config=./config.toml -L debug > $OUT_DIR/shard_diff.output || true
check_contains "as CHECKSUM FROM \`router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`Router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`router_test_0\`.\`Tbl\`" $OUT_DIR/sync_diff.log
#check_not_contains "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains_count "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log 1
check_not_contains "as CHECKSUM FROM \`Router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`Router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log

rm -rf $OUT_DIR/*

echo "test router 3: other rule"
sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_router_3.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml
sync_diff_inspector --config=./config.toml -L debug > $OUT_DIR/shard_diff.output || true
check_not_contains "as CHECKSUM FROM \`router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`Router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`router_test_0\`.\`Tbl\`" $OUT_DIR/sync_diff.log
#check_contains "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains_count "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log 2
check_contains "as CHECKSUM FROM \`Router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`Router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "test router 4: no rule"
sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_router_4.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml
sync_diff_inspector --config=./config.toml -L debug > $OUT_DIR/shard_diff.output || true
check_not_contains "as CHECKSUM FROM \`router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`Router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_not_contains "as CHECKSUM FROM \`router_test_0\`.\`Tbl\`" $OUT_DIR/sync_diff.log
#check_contains "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains_count "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log 2
check_contains "as CHECKSUM FROM \`Router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`Router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "test router 5: regex rule"
sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_router_5.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml
sync_diff_inspector --config=./config.toml -L debug > $OUT_DIR/shard_diff.output || true
check_contains "as CHECKSUM FROM \`router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`Router_test_0\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`router_test_0\`.\`Tbl\`" $OUT_DIR/sync_diff.log
#check_contains "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains_count "as CHECKSUM FROM \`router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log 2
check_contains "as CHECKSUM FROM \`Router_test_1\`.\`tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
check_contains "as CHECKSUM FROM \`Router_test_1\`.\`Tbl\`" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "shard test passed"