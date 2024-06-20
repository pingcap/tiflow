#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run_changefeed() {
	local changefeed_id=$1
	local start_ts=$2
	local should_pass_check=$3

	TOPIC_NAME="ticdc-mq-split-by-partition-key-$changefeed_id"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true&partition-num=6" ;;
	pulsar)
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true&partition-num=6"
		;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config=$CUR/conf/$changefeed_id.toml -c "$changefeed_id"
	sleep 5 # wait for changefeed to start

	# determine the sink uri and run corresponding consumer
	# currently only kafka and pulsar are supported
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI $CUR/conf/$changefeed_id.toml ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI $CUR/conf/$changefeed_id.toml ;;
	esac

	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200

	cp $CUR/conf/diff_config.toml $WORK_DIR/diff_config.toml
	sed -i "s/<suffix>/$changefeed_id/" $WORK_DIR/diff_config.toml
	if [[ $should_pass_check == true ]]; then
		check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 100
	else
		check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 30 && exit 1 || echo "check_sync_diff failed as expected for $changefeed_id"
	fi

	run_sql "drop database if exists test" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
}

# use kafka-consumer with canal-json decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ] && [ "$SINK_TYPE" != "pulsar" ]; then
		return
	fi

	# clean up environment
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	if [ "$SINK_TYPE" == "pulsar" ]; then
		run_pulsar_cluster $WORK_DIR normal
	fi

	# start tidb cluster
	start_tidb_cluster --workdir $WORK_DIR
	# cd $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# test index dispatcher
	run_changefeed "changefeed-index-default-config" $start_ts true
	run_changefeed "changefeed-index-fail" $start_ts false
	run_changefeed "changefeed-index-succ" $start_ts true

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
