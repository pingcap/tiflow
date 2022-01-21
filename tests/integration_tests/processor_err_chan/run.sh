#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_changefeed_mark_normal() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	info=$(cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "normal" ]]; then
		echo "changefeed state $state does not equal to normal"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ "$error_msg" ]]; then
		echo "error message '$message' is not as expected '$error_msg'"
		exit 1
	fi
}

export -f check_changefeed_mark_normal

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="ticdc-processor-err-chan-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	run_sql "CREATE DATABASE processor_err_chan;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE processor_err_chan;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	for i in $(seq 1 10); do
		run_sql "CREATE table processor_err_chan.t$i (id int primary key auto_increment)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "CREATE table processor_err_chan.t$i (id int primary key auto_increment)" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/processor/pipeline/ProcessorAddTableError=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr

	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	retry_time=10
	ensure $retry_time check_changefeed_mark_normal $pd_addr $changefeed_id "processor add table injected error"

	for i in $(seq 1 10); do
		run_sql "INSERT INTO processor_err_chan.t$i values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
