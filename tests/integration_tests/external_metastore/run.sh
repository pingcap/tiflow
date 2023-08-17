#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# test mysql sink only in this case
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql "CREATE table test.external_metastore(id int primary key, val int);"

	# run cdc cluster
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --config $CUR/conf/cdc.toml --cluster-id "test1" --addr "127.0.0.1:8300" --logsuffix external_metastore.server

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"

	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --server "http://127.0.0.1:8300"

	# same dml for table multi_cdc1
	run_sql "INSERT INTO test.external_metastore(id, val) VALUES (1, 1);"
	run_sql "INSERT INTO test.external_metastore(id, val) VALUES (2, 2);"
	run_sql "INSERT INTO test.external_metastore(id, val) VALUES (3, 3);"

	check_table_exists "test.external_metastore" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
