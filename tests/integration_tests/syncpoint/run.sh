#!/bin/bash

# [DISCRIPTION]:
#   This test is related to https://github.com/pingcap/tiflow/issues/658.
#   It will record the syncpoint map of upstream and downstream to a table to achieve snapshot level consistency replication
# [STEP]:
#   1. Create changefeed with --sync-point --sync-interval=10s
#   2. After test, get all syncpoints from tidb_cdc.syncpoint_v1
#   3. Get all ddl history from Upstream TiDB
#   4. Check if the syncpoint is in the process of a ddl, if not, using syncpoint to check data diff of upstream and downstream

set -eu

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

# check whether the given tso happens in a DDL job, if true returns the start_ts
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
			return
		fi
	done
}

# check whether the synpoint happens after test ddl to avoid `no table need to be compared` of sync_diff_inspector
# we need to make sure that the syncpoint we check is after the `CREATE table`
function checkValidSyncPointTs() {
	primary_ts=$1
	first_ddl_ts=$2
	if [[ ($primary_ts < $first_ddl_ts) ]]; then
		echo 0
		return
	fi
	echo 1
}

function checkDiff() {
	primaryArr=($(grep primary_ts $OUT_DIR/sql_res.$TEST_NAME.txt | awk -F ": " '{print $2}'))
	secondaryArr=($(grep secondary_ts $OUT_DIR/sql_res.$TEST_NAME.txt | awk -F ": " '{print $2}'))
	tsos=($(curl -s http://$UP_TIDB_HOST:$UP_TIDB_STATUS/ddl/history | grep -E "real_start_ts|FinishedTS" | grep -oE "[0-9]*"))
	firstDDLTs=($(curl -s http://$UP_TIDB_HOST:$UP_TIDB_STATUS/ddl/history | grep -B 3 "CREATE table testSync" | grep "real_start_ts" | grep -oE "[0-9]*"))
	num=${#primaryArr[*]}
	for ((i = 0; i < $num; i++)); do
		if [[ 1 != $(checkValidSyncPointTs ${primaryArr[$i]} $firstDDLTs) ]]; then
			echo "skip invalid syncpoint primary_ts: ${primaryArr[$i]}, first_ddl_ts: $firstDDLTs"
			continue
		fi

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

	# make suer no panic happen when the syncpoint enable and the ddl sink initializing slowly
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/DDLSinkInitializeSlowly=return(true)'
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	run_sql_file $CUR/data/prepare.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# this test contains `set global tidb_external_ts = ?` , which requires super privilege, so we
	# can't use the normal user
	SINK_URI="mysql://syncpoint:dGVzdDEyMzQ1Ng==@127.0.0.1:3306/?max-txn-row=1"
	run_sql "SET GLOBAL tidb_enable_external_ts_read = on;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	goSql

	check_table_exists "testSync.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "testSync.simple1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "testSync.simple2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql "SET GLOBAL tidb_enable_external_ts_read = off;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
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
