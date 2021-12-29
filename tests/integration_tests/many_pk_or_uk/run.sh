#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	stop_tidb_cluster

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-many-pk-or-uk-test-$RANDOM"
	case $SINK_TYPE in
<<<<<<< HEAD:tests/integration_tests/many_pk_or_uk/run.sh
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
=======
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=3&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760&protocol=canal-json&enable-tidb-extension=true" ;;
>>>>>>> 0418c69c8 (use canal-json with tidb extension for all kafka integration test.):tests/many_pk_or_uk/run.sh
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	if [ "$SINK_TYPE" == "kafka" ]; then
<<<<<<< HEAD:tests/integration_tests/many_pk_or_uk/run.sh
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
=======
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=3&version=${KAFKA_VERSION}&max-message-bytes=10485760&protocol=canal-json&enable-tidb-extension=true"
>>>>>>> 0418c69c8 (use canal-json with tidb extension for all kafka integration test.):tests/many_pk_or_uk/run.sh
	fi
}

trap stop_tidb_cluster EXIT
prepare $*

cd "$(dirname "$0")"
set -o pipefail
GO111MODULE=on go run main.go -config ./config.toml 2>&1 | tee $WORK_DIR/tester.log
check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
check_sync_diff $WORK_DIR $CUR/diff_config.toml
cleanup_process $CDC_BINARY
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
