#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4
MAX_RETRIES=20

function run() {
	# kafka is not supported yet.
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="processor-etcd-delay-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	export GO_FAILPOINTS='github.com/pingcap/tiflow/pkg/orchestrator/ProcessorEtcdDelay=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql "CREATE DATABASE processor_delay;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for i in {1..50}; do
		run_sql "CREATE table processor_delay.t$i(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "INSERT INTO processor_delay.t$i VALUES (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	for i in {1..50}; do
		check_table_exists "processor_delay.t$i" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done

	if grep -Fa "[PANIC]" ${WORK_DIR}/cdc.log; then
		echo "ticdc panic"
		exit 1
	fi
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
