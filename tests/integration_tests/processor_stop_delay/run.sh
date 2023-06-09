#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TABLE_COUNT=3

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="ticdc-processor-stop-delay-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/processor/processorStopDelay=1*sleep(10000)'

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	esac

	run_sql "CREATE DATABASE processor_stop_delay;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table processor_stop_delay.t (id int primary key auto_increment, t datetime DEFAULT CURRENT_TIMESTAMP)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO processor_stop_delay.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "processor_stop_delay.t" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# pause changefeed first, and then resume the changefeed. The processor stop
	# logic will be delayed by 10s, which is controlled by failpoint injection.
	# The changefeed should be resumed and no data loss.
	cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
	sleep 1
	run_sql "INSERT INTO processor_stop_delay.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr
	run_sql "INSERT INTO processor_stop_delay.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
