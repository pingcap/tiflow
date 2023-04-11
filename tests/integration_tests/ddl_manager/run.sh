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

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/ExecuteDDLSlowly=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	# this test contains `recover table`, which requires super privilege, so we
	# can't use the normal user
	TOPIC_NAME="ticdc-ddl-mamager-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://root@127.0.0.1:3306/" ;;
	esac
	changefeed_id="ddl-manager"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id}

	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# kill owner to make sure ddl manager is working right when owner is down and up
	kill_cdc_pid $owner_pid
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	check_table_exists ddl_manager.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	# make sure all tables are equal in upstream and downstream
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 180
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
