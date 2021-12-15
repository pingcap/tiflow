#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

function prepare() {
	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"
	stop_tidb_cluster
	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="local://$WORK_DIR/test?"

	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
}

success=0
function check_cdclog() {
	# TODO test rotate file
	DATA_DIR="$WORK_DIR/test"
	# retrieve table id by log meta
	if [ ! -f $DATA_DIR/log.meta ]; then
		return
	fi
	table_id=$(cat $DATA_DIR/log.meta | jq | grep t1 | awk -F '"' '{print $2}')
	if [ ! -d $DATA_DIR/t_$table_id ]; then
		return
	fi
	file_count=$(ls -ahl $DATA_DIR/t_$table_id | grep cdclog | wc -l)
	if [[ ! "$file_count" -eq "1" ]]; then
		echo "$TEST_NAME failed, expect 1 row changed files, obtain $file_count"
		return
	fi
	if [ ! -d $DATA_DIR/ddls ]; then
		return
	fi
	ddl_file_count=$(ls -ahl $DATA_DIR/ddls | grep ddl | wc -l)
	if [[ ! "$ddl_file_count" -eq "1" ]]; then
		echo "$TEST_NAME failed, expect 1 ddl file, obtain $ddl_file_count"
		return
	fi
	success=1
}

function cdclog_test() {
	run_sql "drop database if exists $TEST_NAME"
	run_sql "create database $TEST_NAME"
	run_sql "create table $TEST_NAME.t1 (c0 int primary key, payload varchar(1024));"
	run_sql "create table $TEST_NAME.t2 (c0 int primary key, payload varchar(1024));"

	run_sql "insert into $TEST_NAME.t1 values (1, 'a')"
	run_sql "insert into $TEST_NAME.t1 values (2, 'b')"

	i=0
	while [ $i -lt 30 ]; do
		check_cdclog
		if [ "$success" == 1 ]; then
			echo "check log successfully"
			break
		fi
		i=$(($i + 1))
		echo "check log failed $i-th time, retry later"
		sleep 2
	done
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
prepare $*
cdclog_test $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
