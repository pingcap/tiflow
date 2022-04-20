#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# TODO: kafka sink has bug with this case, remove this after bug is fixed
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/mysql/SinkFlushDMLPanic=return(true);github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka/SinkFlushDMLPanic=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"

	TOPIC_NAME="ticdc-processor-resolved-ts-fallback-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://kafka01:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --sink-uri="$SINK_URI"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_sql "CREATE database processor_resolved_ts_fallback;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table processor_resolved_ts_fallback.t1(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# wait table t1 is processed by cdc server
	ensure 10 "cdc cli processor list|jq '.|length'|grep -E '^1$'"
	# check the t1 is replicated to downstream to make sure the t1 is dispatched to cdc1
	check_table_exists "processor_resolved_ts_fallback.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8302" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
	run_sql "CREATE table processor_resolved_ts_fallback.t2(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table processor_resolved_ts_fallback.t3(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "processor_resolved_ts_fallback.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "processor_resolved_ts_fallback.t3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	ensure 10 "cdc cli processor list|jq '.|length'|grep -E '^2$'"

	run_sql "INSERT INTO processor_resolved_ts_fallback.t1 values (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# wait cdc server 1 is panic
	ensure 10 "cdc cli capture list|jq '.|length'|grep -E '^1$'"
	run_sql "INSERT INTO processor_resolved_ts_fallback.t1 values (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO processor_resolved_ts_fallback.t2 values (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
