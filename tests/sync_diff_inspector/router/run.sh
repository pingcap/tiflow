#!/bin/sh

set -ex

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

# prepare upstream TiDB table and downstream MySQL table with route mapping:
# route_up_test.t_route -> route_down_test.t_route
mysql -uroot -h 127.0.0.1 -P 4001 -e "drop database if exists route_up_test;"
mysql -uroot -h 127.0.0.1 -P 4001 -e "drop database if exists route_down_test;"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create database route_up_test;"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create table route_up_test.t_route(id int primary key, val varchar(20));"
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into route_up_test.t_route values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j');"

mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "drop database if exists route_down_test;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create database route_down_test;"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table route_down_test.t_route(id int primary key, val varchar(20));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into route_down_test.t_route values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'x'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j');"

sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" >./config.toml

export GO_FAILPOINTS="main/binsearchSplitThreshold=return(4)"
sync_diff_inspector --config=./config.toml -L debug >$OUT_DIR/router.output || true
export GO_FAILPOINTS=""

check_contains "check failed!!!" $OUT_DIR/sync_diff.log
check_contains "A total of 1 tables have been compared, 0 tables finished, 1 tables failed, 0 tables skipped." $OUT_DIR/router.output
check_contains "+1/-1" $OUT_DIR/summary.txt

check_contains "get mid by size" $OUT_DIR/sync_diff.log
grep "get mid by size" $OUT_DIR/sync_diff.log >$OUT_DIR/router_mid.log
check_contains "FROM \`route_up_test\`.\`t_route\`" $OUT_DIR/router_mid.log
check_not_contains "FROM \`route_down_test\`.\`t_route\`" $OUT_DIR/router_mid.log
