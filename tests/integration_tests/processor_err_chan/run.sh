#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="ticdc-processor-err-chan-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar) SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	run_sql "CREATE DATABASE processor_err_chan;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE processor_err_chan;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	for i in $(seq 1 10); do
		run_sql "CREATE table processor_err_chan.t$i (id int primary key auto_increment)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "CREATE table processor_err_chan.t$i (id int primary key auto_increment)" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/processor/sourcemanager/puller/ProcessorAddTableError=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr --config $CUR/conf/server.toml

	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer $WORK_DIR $SINK_URI ;;
	esac

	retry_time=10
	ensure $retry_time check_changefeed_state $pd_addr $changefeed_id "normal" "null" ""

	for i in $(seq 1 10); do
		run_sql "INSERT INTO processor_err_chan.t$i values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs_contains $WORK_DIR "processor add table injected error"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
