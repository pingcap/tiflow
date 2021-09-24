#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	sudo pip install -U requests==2.26.0

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	# wait for cdc run
	sleep 1

	TOPIC_NAME="ticdc-http-api-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	python $CUR/util/test_case.py check_health
	python $CUR/util/test_case.py get_status

	python $CUR/util/test_case.py create_changefeed "$SINK_URI"

	run_sql "CREATE table test.simple(id int primary key, val int);"
	run_sql "CREATE table test.\`simple-dash\`(id int primary key, val int);"
	# wait for above sql done in the up source
	sleep 1

	sequential_cases=(
		"list_changefeed"
		"get_changefeed"
		"pause_changefeed"
		"update_changefeed"
		"resume_changefeed"
		"rebalance_table"
		"move_table"
		"get_processor"
		"list_processor"
		"set_log_level"
		"remove_changefeed"
		"resign_owner"
	)

	for case in $sequential_cases; do {
		python $CUR/util/test_case.py "$case"
	}; done

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
