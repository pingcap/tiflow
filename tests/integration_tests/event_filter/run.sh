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

	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"

	TOPIC_NAME="ticdc-event-filter-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar) SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	# create changefeed
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --server="127.0.0.1:8300" --config=$CUR/conf/cf.toml

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer $WORK_DIR $SINK_URI ;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# make suer table t1 is deleted in upstream and exists in downstream
	check_table_not_exists "event_filter.t1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "event_filter.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_normal" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_truncate" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_alter" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# check those rows that are not filtered are synced to downstream
	run_sql "select count(1) from event_filter.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 2"
	run_sql "select count(2) from event_filter.t1 where id=1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(2): 1"
	run_sql "select count(3) from event_filter.t1 where id=2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(3): 0"
	run_sql "select count(4) from event_filter.t1 where id=3;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(4): 0"
	run_sql "select count(5) from event_filter.t1 where id=4;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(5): 1"

	run_sql "TRUNCATE TABLE event_filter.t_truncate;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql_file $CUR/data/test_truncate.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE event_filter.t_alter MODIFY t_bigint BIGINT;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql_file $CUR/data/test_alter.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create table event_filter.finish_mark(id int primary key);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# check table t_normal is replicated
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
