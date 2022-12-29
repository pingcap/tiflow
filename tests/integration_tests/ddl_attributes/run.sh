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

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-ddl-attributes-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}" ;;
	*) SINK_URI="mysql://root@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}"
	fi
	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	sleep 3
	check_table_exists ddl_attributes.attributes_t1_new ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists ddl_attributes.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY

	run_sql "show create table ddl_attributes.placement_t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "show create table ddl_attributes.placement_t2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "show create table ddl_attributes.placement_t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "CREATE TABLE \`placement_t1\` "
	run_sql "show create table ddl_attributes.placement_t2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "CREATE TABLE \`placement_t2\` "

  TTL_MARK='![ttl]'
  CREATE_TTL_SQL_CONTAINS="/*T${TTL_MARK} TTL=\`t\` + INTERVAL 1 DAY */ /*T${TTL_MARK} TTL_ENABLE='OFF' */"

	run_sql "show create table ddl_attributes.ttl_t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
    check_contains "$CREATE_TTL_SQL_CONTAINS"
	run_sql "show create table ddl_attributes.ttl_t2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
    check_contains "$CREATE_TTL_SQL_CONTAINS"
	run_sql "show create table ddl_attributes.ttl_t3;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
    check_contains "$CREATE_TTL_SQL_CONTAINS"
	run_sql "show create table ddl_attributes.ttl_t4;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
    check_not_contains "TTL_ENABLE"
    check_not_contains "TTL="
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
