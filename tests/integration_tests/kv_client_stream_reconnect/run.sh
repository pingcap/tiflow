#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TABLE_COUNT=10

# This test mainly verifies kv client force reconnect can work
# Trigger force reconnect by failpoint injection
function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="kv-client-stream-reconnect-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	# this will be triggered every 5s in kv client
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/kv/kvClientForceReconnect=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_sql "CREATE DATABASE kv_client_stream_reconnect;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	for i in $(seq $TABLE_COUNT); do
		run_sql "create table kv_client_stream_reconnect.t$i (id int primary key auto_increment, a int default 10);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	for i in $(seq 30); do
		tbl="t$((1 + $RANDOM % $TABLE_COUNT))"
		run_sql "insert into kv_client_stream_reconnect.$tbl values (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		sleep 1
	done

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
