#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=10

function get_safepoint() {
	pd_addr=$1
	pd_cluster_id=$2
	safe_point=$(ETCDCTL_API=3 etcdctl --endpoints=$pd_addr get /pd/$pd_cluster_id/gc/safe_point/service/ticdc-default --prefix | grep -oE "safe_point\":[0-9]+" | grep -oE "[0-9]+")
	echo $safe_point
}

function clear_gc_worker_safepoint() {
	pd_addr=$1
	pd_cluster_id=$2
	ETCDCTL_API=3 etcdctl --endpoints=$pd_addr del /pd/$pd_cluster_id/gc/safe_point/service/ticdc
}

function check_safepoint_cleared() {
	pd_addr=$1
	pd_cluster_id=$2
	query=$(ETCDCTL_API=3 etcdctl --endpoints=$pd_addr get /pd/$pd_cluster_id/gc/safe_point/service/ticdc)
	if [ ! -z "$query" ]; then
		echo "gc safepoint is not cleared: $query"
	fi
}

function check_safepoint_forward() {
	pd_addr=$1
	pd_cluster_id=$2
	safe_point1=$3
	sleep 1
	safe_point2=$(get_safepoint $pd_addr $pd_cluster_id)
	if [[ "$safe_point1" == "$safe_point2" ]]; then
		echo "safepoint $safe_point1 is not forward"
		exit 1
	fi
}

function check_safepoint_equal() {
	pd_addr=$1
	pd_cluster_id=$2
	safe_point1=$(get_safepoint $pd_addr $pd_cluster_id)
	for i in $(seq 1 3); do
		sleep 1
		safe_point2=$(get_safepoint $pd_addr $pd_cluster_id)
		if [[ "$safe_point1" != "$safe_point2" ]]; then
			echo "safepoint is unexpected forward: $safe_point1 -> $safe_point2"
			exit 1
		fi
	done
}

export -f get_safepoint
export -f check_safepoint_forward
export -f check_safepoint_cleared
export -f check_safepoint_equal
export -f clear_gc_worker_safepoint

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="ticdc-gc-safepoint-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	export GO_FAILPOINTS='github.com/pingcap/tiflow/pkg/txnutil/gc/InjectGcSafepointUpdateInterval=return(500)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	pd_cluster_id=$(curl -s $pd_addr/pd/api/v1/cluster | grep -oE "id\":\s[0-9]+" | grep -oE "[0-9]+")
	clear_gc_worker_safepoint $pd_addr $pd_cluster_id

	run_sql "CREATE DATABASE gc_safepoint;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table gc_safepoint.simple(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO gc_safepoint.simple VALUES (),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	start_safepoint=$(get_safepoint $pd_addr $pd_cluster_id)
	ensure $MAX_RETRIES check_safepoint_forward $pd_addr $pd_cluster_id $start_safepoint

	# after the changefeed is paused, the safe_point will be not updated
	cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
	ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "stopped" "null" ""
	ensure $MAX_RETRIES check_safepoint_equal $pd_addr $pd_cluster_id

	# resume changefeed will recover the safe_point forward
	cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr
	ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "normal" "null" ""
	start_safepoint=$(get_safepoint $pd_addr $pd_cluster_id)
	ensure $MAX_RETRIES check_safepoint_forward $pd_addr $pd_cluster_id $start_safepoint

	cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
	ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "stopped" "null" ""
	# create another changefeed, because there exists a paused changefeed,
	# the safe_point still does not forward
	changefeed_id2=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id2 "normal" "null" ""
	ensure $MAX_RETRIES check_safepoint_equal $pd_addr $pd_cluster_id

	# remove paused changefeed, the safe_point forward will recover
	cdc cli changefeed remove --changefeed-id=$changefeed_id --pd=$pd_addr
	start_safepoint=$(get_safepoint $pd_addr $pd_cluster_id)
	ensure $MAX_RETRIES check_safepoint_forward $pd_addr $pd_cluster_id $start_safepoint

	# remove all changefeeds, the safe_point will be cleared
	cdc cli changefeed remove --changefeed-id=$changefeed_id2 --pd=$pd_addr
	ensure $MAX_RETRIES check_safepoint_cleared $pd_addr $pd_cluster_id

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
