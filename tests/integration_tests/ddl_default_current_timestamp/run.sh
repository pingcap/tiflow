#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
DB_NAME=ddl_default_current_timestamp
TIME_ZONE=UTC

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	# Force a single session and skip set/reset for targeted DDLs to simulate leakage.
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql/MySQLSinkForceSingleConnection=return(true);github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql/MySQLSinkSkipResetSessionTimestampAfterDDL=return("c2");github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql/MySQLSinkSkipSetSessionTimestamp=return("d2")'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="mysql://normal:123456@127.0.0.1:3306/"
	export GO_FAILPOINTS=''

	run_sql "DROP DATABASE IF EXISTS ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.t (id int primary key, v int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.t VALUES (1, 10), (2, 20), (3, 30);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE ${DB_NAME}.t SET v = v + 1 WHERE id = 2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.t VALUES (4, 40);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "SET time_zone = '${TIME_ZONE}'; SET @@timestamp = 1000000000.123456; ALTER TABLE ${DB_NAME}.t ADD COLUMN c DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "SET time_zone = '${TIME_ZONE}'; SET @@timestamp = 1000000001.654321; INSERT INTO ${DB_NAME}.t (id, v) VALUES (5, 50);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.t (id, v, c) VALUES (6, 60, '2001-09-09 01:46:40.123456');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "SET time_zone = '${TIME_ZONE}'; SET @@timestamp = 1000000002.789012; UPDATE ${DB_NAME}.t SET v = v + 1, c = CURRENT_TIMESTAMP(6) WHERE id = 1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "SET time_zone = '${TIME_ZONE}'; SET @@timestamp = 1000000003.111111; UPDATE ${DB_NAME}.t SET c = DEFAULT WHERE id = 2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DELETE FROM ${DB_NAME}.t WHERE id = 4;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 120

	run_sql "DROP TABLE IF EXISTS ${DB_NAME}.t_fp;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.t_fp (id int primary key, v int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.t_fp VALUES (1, 10), (2, 20);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "SET time_zone = '${TIME_ZONE}'; SET @@timestamp = 1000000000.123456; ALTER TABLE ${DB_NAME}.t_fp ADD COLUMN c2 DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	expected_ts="2001-09-09 01:46:40.123456"
	down_c2=""
	for i in $(seq 1 30); do
		down_c2=$(mysql -uroot -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} --default-character-set utf8mb4 --init-command="set time_zone='${TIME_ZONE}'" -Nse "SELECT c2 FROM ${DB_NAME}.t_fp WHERE id=1;" 2>/dev/null || true)
		if [ "$down_c2" = "$expected_ts" ]; then
			break
		fi
		sleep 2
	done
	if [ "$down_c2" != "$expected_ts" ]; then
		echo "downstream c2 mismatch: expected ${expected_ts}, got ${down_c2}"
		exit 1
	fi

	run_sql "SET time_zone = '${TIME_ZONE}'; SET @@timestamp = 1000000001.654321; ALTER TABLE ${DB_NAME}.t_fp ADD COLUMN d2 DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	down_d2=""
	for i in $(seq 1 30); do
		down_d2=$(mysql -uroot -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} --default-character-set utf8mb4 --init-command="set time_zone='${TIME_ZONE}'" -Nse "SELECT d2 FROM ${DB_NAME}.t_fp WHERE id=1;" 2>/dev/null || true)
		if [ -n "$down_d2" ] && [ "$down_d2" != "$expected_ts" ]; then
			break
		fi
		sleep 2
	done
	if [ -z "$down_d2" ] || [ "$down_d2" = "$expected_ts" ]; then
		echo "downstream d2 should not reuse stale timestamp: expected != ${expected_ts}, got ${down_d2}"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

# Only MySQL sink applies the DDL directly with full column metadata.
if [ "$SINK_TYPE" == "mysql" ]; then
	trap stop_tidb_cluster EXIT
	run $*
	check_logs_contains $WORK_DIR "Skip setting session timestamp due to failpoint"
	check_logs_contains $WORK_DIR "Skip resetting session timestamp after DDL execution due to failpoint"
	check_logs $WORK_DIR
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
