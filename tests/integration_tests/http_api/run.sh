#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=50

function run() {
	# mysql and kafka are the same
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	sudo python3 -m pip install -U requests==2.26.0

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	# wait for cdc run
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8301"
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep -v \"$owner_id\" | grep id"
	capture_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")
	echo "capture_id:" $capture_id

	python3 $CUR/util/test_case.py check_health
	python3 $CUR/util/test_case.py get_status

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	python3 $CUR/util/test_case.py create_changefeed "$SINK_URI"

	run_sql "CREATE table test.simple(id int primary key, val int);"
	run_sql "CREATE table test.\`simple-dash\`(id int primary key, val int);"
	run_sql "CREATE table test.simple1(id int primary key, val int);"
	run_sql "CREATE table test.simple2(id int primary key, val int);"
	run_sql "INSERT INTO test.simple1(id, val) VALUES (1, 1);"
	run_sql "INSERT INTO test.simple1(id, val) VALUES (2, 2);"

	# wait for above sql done in the up source
	sleep 2

	check_table_exists test.simple1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	sequential_cases1=(
		"list_changefeed"
		"get_changefeed"
		"pause_changefeed"
		"update_changefeed"
	)

	for case in ${sequential_cases1[@]}; do
		python3 $CUR/util/test_case.py "$case"
	done

	# kill the cdc owner server
	kill $owner_pid
	# check that the new owner is elected
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 |grep $capture_id -A1 | grep '\"is-owner\": true'"
	# restart the old owner capture
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"address\": \"127.0.0.1:8300\"'"

	# make sure api works well after one owner was killed and another owner was elected
	sequential_cases2=(
		"list_changefeed"
		"get_changefeed"
		"resume_changefeed"
		"pause_changefeed"
		"rebalance_table"
		"move_table"
		"get_processor"
		"list_processor"
		"set_log_level"
		"remove_changefeed"
		"resign_owner"
	)

	for case in ${sequential_cases2[@]}; do
		python3 $CUR/util/test_case.py "$case"
	done
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
