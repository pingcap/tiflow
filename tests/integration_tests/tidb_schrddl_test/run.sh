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

	TOPIC_NAME="ticdc-default-value-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi
}

trap stop_tidb_cluster EXIT
prepare $*

cd "$(dirname "$0")"
set -o pipefail

# go get schrddl project and run
go get github.com/PingCAP-QE/schrddl
GO111MODULE=on go build -o ./schrddl github.com/PingCAP-QE/schrddl
./schrddl --mode=parallel --db="test" --time=30s --concurrency=5 --output='schrddl.out'
# write fininsh mark
mysql -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -uroot -e "create table test.finish_mark(id int)"
check_table_exists 'test.finish_mark' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 600
check_sync_diff $WORK_DIR $CUR/diff_config.toml
cleanup_process $CDC_BINARY
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
