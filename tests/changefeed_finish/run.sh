#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=10

function check_changefeed_is_finished() {
	pd=$1
	changefeed=$2
	query=$(cdc cli changefeed query -c=$changefeed)
	echo "$query"
	state=$(echo "$query" | jq ".state" | tr -d '"')
	if [[ ! "$state" -eq "finished" ]]; then
		echo "state $state is not finished"
		exit 1
	fi

	status_length=$(echo "$query" | sed "/has been deleted/d" | jq '."task-status"|length')
	if [[ ! "$status_length" -eq "0" ]]; then
		echo "unexpected task status length $status_length, should be 0"
		exit 1
	fi
}

export -f check_changefeed_is_finished

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="ticdc-changefeed-pause-resume-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	now=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	# 90s after now
	target_ts=$(($now + 90 * 10 ** 3 * 2 ** 18))
	changefeed_id=$(cdc cli changefeed create --sink-uri="$SINK_URI" --target-ts=$target_ts 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_sql "CREATE DATABASE changefeed_finish;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table changefeed_finish.t (id int primary key auto_increment, t datetime DEFAULT CURRENT_TIMESTAMP)"
	for i in $(seq 1 10); do
		run_sql "insert into changefeed_finish.t values (),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	sleep 90

	ensure $MAX_RETRIES check_changefeed_is_finished $pd_addr $changefeed_id

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
