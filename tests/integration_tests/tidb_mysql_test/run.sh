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
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	#run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	SINK_PARA="{\"force_replicate\":true, \"changefeed_id\":\"tidb-mysql-test\", \"sink_uri\":\"$SINK_URI\", \"start_ts\":$start_ts}"
	curl -X POST -H "'Content-type':'application/json'" http://127.0.0.1:8300/api/v1/changefeeds -d "$SINK_PARA"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	esac
}

# kafka and storage test is not supported yet.
# Because:(1) most cases has no pk/uk, consumer will receive more than one same DML
#         (2) kafka consumer need support force_replicate
if [ "$SINK_TYPE" != "mysql" ]; then
	echo "[$(date)] <<<<<< skip test case $TEST_NAME for kafka! >>>>>>"
	exit 0
fi

# mysql test may suffer from duplicate DML for no pk/uk
# [TODO] need add pk or move from integration test
trap stop_tidb_cluster EXIT
prepare $*
cd "$(dirname "$0")"
# run mysql-test cases
./test.sh
mysql -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -uroot -e "create table test.finish_mark(id int primary key)"
check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
check_sync_diff $WORK_DIR $CUR/diff_config.toml
cleanup_process $CDC_BINARY
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
