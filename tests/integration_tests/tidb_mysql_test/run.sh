#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
TIDB_CONFIG=$CUR/tidb_config.toml
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	stop_tidb_cluster
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	# Use '--tidb-config' here to suppress the default config for other integration tests
	# Maybe it can specify some options for the rest test cases in the future
	start_tidb_cluster --workdir $WORK_DIR --tidb-config $TIDB_CONFIG

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-tidb-mysql-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	SINK_PARA="{\"force_replicate\":true, \"changefeed_id\":\"tidb-mysql-test\", \"sink_uri\":\"$SINK_URI\", \"start_ts\":$start_ts}"
	curl -X POST -H "'Content-type':'application/json'" http://$CDC_HOST:$CDC_PORT/api/v1/changefeeds -d "$SINK_PARA"
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

cp -f $CUR/converter.sh $MYSQL_TEST_PATH
cp -f $CUR/build.sh $MYSQL_TEST_PATH
cd $MYSQL_TEST_PATH

# convert test case
cases="bigint composite_index date_formats datetime_insert \
    datetime_update drop concurrent_ddl gcol_alter_table \
    partition_bug18198 update_stmt alter_table alter_table_PK\
    partition_list partition_range single_delete_update time \
    timestamp_insert timestamp_update type_decimal transaction_isolation_func\
    type_time type_timestamp type_uint update \
    insert_select insert_update alter_table1 json\
    mysql_replace date_time_ddl"
./converter.sh "$cases"

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
check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
check_sync_diff $WORK_DIR $CUR/diff_config.toml
cleanup_process $CDC_BINARY
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
