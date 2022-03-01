#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	stop_tidb_cluster
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR
	
    cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-tidb-mysql-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	#run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	SINK_PARA="{\"force_replicate\":true, \"changefeed_id\":\"tidb-mysql-test\", \"sink_uri\":\"$SINK_URI\", \"start_ts\":$start_ts}"
	curl -X POST -H "'Content-type':'application/json'" http://127.0.0.1:8300/api/v1/changefeeds -d "$SINK_PARA"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi
}

# kafka test is not supported yet.
# Because:(1) most cases has no pk/uk, consumer will receive more than one same DML
#         (2) kafka consumer need support force_replicate
if [ "$SINK_TYPE" = "kafka" ]; then
	echo "[$(date)] <<<<<< skip test case $TEST_NAME for kafka! >>>>>>"
	exit 0
fi

# mysql test may suffer from duplicate DML for no pk/uk
trap stop_tidb_cluster EXIT

# setup cluster
echo -e "start upstream and downstream cluster"
prepare $*

MYSQL_TEST_PATH=$CUR/tidb_test/mysql_test

cp -f $CUR/converter2.sh $MYSQL_TEST_PATH
cp -f $CUR/build.sh $MYSQL_TEST_PATH
cd $MYSQL_TEST_PATH

# convert test case
cases="bigint composite_index date_formats datetime_insert \
    datetime_update drop concurrent_ddl gcol_alter_table \
    partition_bug18198 update_stmt\
    partition_list partition_range single_delete_update time \
    timestamp_insert timestamp_update type_decimal \
    type_time type_timestamp type_uint update"

./converter2.sh "$cases"
# run mysql-test cases
echo -e "mysql_test start\n"
TEST_BIN_PATH=./mysql_test
./build.sh
echo "run mysql test cases:${cases}"
"$TEST_BIN_PATH" --host=${UP_TIDB_HOST} --port=${UP_TIDB_PORT} --log-level=error --reserve-schema=true ${cases}
echo "mysqltest end"

cd $CUR
mysql -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -uroot -e "create table test.finish_mark(id int primary key)"
# all tests will take too much time, how to optimize it?
check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 2000
check_sync_diff $WORK_DIR $CUR/diff_config.toml
cleanup_process $CDC_BINARY
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
