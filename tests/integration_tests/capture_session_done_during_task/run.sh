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
	TOPIC_NAME="ticdc-capture-session-done-during-task-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	# create database and table in both upstream and downstream to ensure there
	# will be task dispatched after changefeed starts.
	run_sql "CREATE DATABASE capture_session_done_during_task;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table capture_session_done_during_task.t (id int primary key auto_increment, a int)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE capture_session_done_during_task;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE table capture_session_done_during_task.t (id int primary key auto_increment, a int)" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "INSERT INTO capture_session_done_during_task.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/processor/processorManagerHandleNewChangefeedDelay=sleep(2000)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	# wait task is dispatched
	sleep 1

	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	capture_key=$(ETCDCTL_API=3 etcdctl get /tidb/cdc/capture --prefix | head -n 1)
	lease=$(ETCDCTL_API=3 etcdctl get $capture_key -w json | grep -o 'lease":[0-9]*' | awk -F: '{print $2}')
	lease_hex=$(printf '%x\n' $lease)
	# revoke lease of etcd capture key to simulate etcd session done
	ETCDCTL_API=3 etcdctl lease revoke $lease_hex

	# capture handle task delays 10s, minus 2s wait task dispatched
	sleep 1
	check_table_exists "capture_session_done_during_task.t" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_sql "INSERT INTO capture_session_done_during_task.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
