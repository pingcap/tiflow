#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

# The follow `sleep 2` make sure the ddl and dml operation can always execute during the test.
# So every syncpoint will have different data and the diff tool can judge the syncpoint's validity.
function ddl() {
	run_sql "DROP table IF EXISTS testSync.simple1"
	sleep 2
	run_sql "DROP table IF EXISTS testSync.simple2"
	sleep 2
	run_sql "CREATE table testSync.simple1(id int primary key, val int);"
	sleep 2
	run_sql "INSERT INTO testSync.simple1(id, val) VALUES (1, 1);"
	sleep 2
	run_sql "INSERT INTO testSync.simple1(id, val) VALUES (2, 2);"
	sleep 2
	run_sql "INSERT INTO testSync.simple1(id, val) VALUES (3, 3);"
	sleep 2
	run_sql "INSERT INTO testSync.simple1(id, val) VALUES (4, 4);"
	sleep 2
	run_sql "INSERT INTO testSync.simple1(id, val) VALUES (5, 5);"
	sleep 2
	run_sql "INSERT INTO testSync.simple1(id, val) VALUES (6, 6);"
	sleep 2
	run_sql "CREATE table testSync.simple2(id int primary key, val int);"
	sleep 2
	run_sql "INSERT INTO testSync.simple2(id, val) VALUES (1, 1);"
	sleep 2
	run_sql "INSERT INTO testSync.simple2(id, val) VALUES (2, 2);"
	sleep 2
	run_sql "INSERT INTO testSync.simple2(id, val) VALUES (3, 3);"
	sleep 2
	run_sql "INSERT INTO testSync.simple2(id, val) VALUES (4, 4);"
	sleep 2
	run_sql "INSERT INTO testSync.simple2(id, val) VALUES (5, 5);"
	sleep 2
	run_sql "INSERT INTO testSync.simple2(id, val) VALUES (6, 6);"
	sleep 2
	run_sql "CREATE index simple1_val ON testSync.simple1(val);"
	sleep 2
	run_sql "CREATE index simple2_val ON testSync.simple2(val);"
	sleep 2
	run_sql "DELETE FROM testSync.simple1 where id=1;"
	sleep 2
	run_sql "DELETE FROM testSync.simple2 where id=1;"
	sleep 2
	run_sql "DELETE FROM testSync.simple1 where id=2;"
	sleep 2
	run_sql "DELETE FROM testSync.simple2 where id=2;"
	sleep 2
	run_sql "DROP index simple1_val ON testSync.simple1;"
	sleep 2
	run_sql "DROP index simple2_val ON testSync.simple2;"
	sleep 2
}

function goSql() {
	for i in {1..3}; do
		go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=testSync
		sleep 2
		ddl
		sleep 2
	done
}

function deployConfig() {
	cat $CUR/conf/diff_config_part1.toml >$CUR/conf/diff_config.toml
	echo "snapshot = \"$1\"" >>$CUR/conf/diff_config.toml
	cat $CUR/conf/diff_config_part2.toml >>$CUR/conf/diff_config.toml
	echo "snapshot = \"$2\"" >>$CUR/conf/diff_config.toml
}

# check wheter the given tso happens in a DDL job, if true returns the start_ts
# and commit_ts of the DDL job
function checkPrimaryTsNotInDDL() {
	primary_ts=$1
	tsos=$2
	count=$((${#tsos[@]} / 2))
	for i in $(seq 1 $count); do
		start_ts=${tsos[((2 * $i - 2))]}
		commit_ts=${tsos[((2 * $i - 1))]}
		if [[ ($primary_ts > $start_ts) && ($primary_ts < $commit_ts) ]]; then
			echo "$start_ts $commit_ts"
		fi
	done
}

function checkDiff() {
	primaryArr=($(grep primary_ts $OUT_DIR/sql_res.$TEST_NAME.txt | awk -F ": " '{print $2}'))
	secondaryArr=($(grep secondary_ts $OUT_DIR/sql_res.$TEST_NAME.txt | awk -F ": " '{print $2}'))
	tsos=($(curl -s http://$UP_TIDB_HOST:$UP_TIDB_STATUS/ddl/history | grep -E "start_ts|FinishedTS" | grep -oE "[0-9]*"))
	num=${#primaryArr[*]}
	for ((i = 0; i < $num; i++)); do
		check_in_ddl=$(checkPrimaryTsNotInDDL ${primaryArr[$i]} $tsos)
		if [[ ! -z $check_in_ddl ]]; then
			echo "syncpoint ${primaryArr[$i]} ${secondaryArr[$i]} " \
				"is recorded in a DDL event(${check_in_ddl[0]}), skip the check of it"
			continue
		fi
		deployConfig ${primaryArr[$i]} ${secondaryArr[$i]}
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	done
	rm $CUR/conf/diff_config.toml
}

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "kafka downstream isn't support syncpoint record"
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	run_sql "CREATE DATABASE testSync;"
	run_sql "CREATE DATABASE testSync;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --sync-point --sync-interval=10s

	goSql

	check_table_exists "testSync.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "testSync.simple1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "testSync.simple2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	sleep 60

	run_sql "SELECT primary_ts, secondary_ts FROM tidb_cdc.syncpoint_v1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	echo "____________________________________"
	cat "$OUT_DIR/sql_res.$TEST_NAME.txt"
	checkDiff
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_final.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
