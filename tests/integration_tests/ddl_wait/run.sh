#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This test simulates DDL operations that take a long time.
# TiCDC blocks DDL operations until its state is not running, except for adding indexes.
# TiCDC also checks add index ddl state before execute a new DDL.
function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	cd $CUR
	GO111MODULE=on go run test.go

	TOPIC_NAME="ticdc-ddl-wait-test-$RANDOM"
	SINK_URI="mysql://root@127.0.0.1:3306/?read-timeout=300ms"

	changefeed_id="ddl-wait"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id}

	run_sql "update test.t set col = 11 where id = 1;"
	run_sql "alter table test.t modify column col decimal(30,10);"
	run_sql "update test.t set col = 22 where id = 2;"
	run_sql "alter table test.t add index (col);"
	# The downstream add index DDL may finish quickly with fast reorg enabled,
	# so we need a short fixed-interval polling to avoid missing the running window.
	for i in $(seq 1 120); do
		run_sql 'SELECT JOB_ID FROM information_schema.ddl_jobs WHERE DB_NAME = "test" AND TABLE_NAME = "t" AND JOB_TYPE LIKE "add index%" AND (STATE = "running" OR STATE = "queueing") LIMIT 1;' \
			"${DOWN_TIDB_HOST}" "${DOWN_TIDB_PORT}" >/dev/null 2>&1 || true
		if check_contains 'JOB_ID:' >/dev/null 2>&1; then
			break
		fi
		sleep 0.5
	done
	check_contains 'JOB_ID:'
	run_sql "update test.t set col = 33 where id = 3;"
	run_sql "create table test.finish_mark (a int primary key);"
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	# make sure all tables are equal in upstream and downstream
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 180
	check_logs_contains $WORK_DIR "DDL replicate success"
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
