#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$(cd $CUR/../_certificates && pwd)
MAX_RETRIES=20

function run() {
	# mysql and kafka are the same
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	sudo python3 -m pip install -U requests==2.26.0

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR
	start_tls_tidb_cluster --workdir $WORK_DIR --tlsdir $TLS_DIR

	cd $WORK_DIR

	echo " \
  [security]
   ca-path = \"$TLS_DIR/ca.pem\"
   cert-path = \"$TLS_DIR/server.pem\"
   key-path = \"$TLS_DIR/server-key.pem\"
   cert-allowed-cn = [\"fake_cn\"]
  " >$WORK_DIR/server.toml

	run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--logsuffix "_${TEST_NAME}_tls1" \
		--pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
		--addr "127.0.0.1:8300" \
		--config "$WORK_DIR/server.toml" \
		--tlsdir "$TLS_DIR" \
		--cert-allowed-cn "client" # The common name of client.pem

	sleep 2

	run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--logsuffix "_${TEST_NAME}_tls2" \
		--pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
		--addr "127.0.0.1:8301" \
		--config "$WORK_DIR/server.toml" \
		--tlsdir "$TLS_DIR" \
		--cert-allowed-cn "client" # The common name of client.pem

	# wait for cdc run
	sleep 2

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"

	python3 $CUR/util/test_case.py check_health $TLS_DIR
	python3 $CUR/util/test_case.py get_status $TLS_DIR

	python3 $CUR/util/test_case.py create_changefeed $TLS_DIR "$SINK_URI"
	# wait for all changefeed created
	ensure $MAX_RETRIES check_changefeed_state "https://${TLS_PD_HOST}:${TLS_PD_PORT}" "changefeed-test1" "normal" "null" ${TLS_DIR}
	ensure $MAX_RETRIES check_changefeed_state "https://${TLS_PD_HOST}:${TLS_PD_PORT}" "changefeed-test2" "normal" "null" ${TLS_DIR}
	ensure $MAX_RETRIES check_changefeed_state "https://${TLS_PD_HOST}:${TLS_PD_PORT}" "changefeed-test3" "normal" "null" ${TLS_DIR}

	# test processor query with no attached tables
	#TODO: comment this test temporary
	#python $CUR/util/test_case.py get_processor $TLS_DIR

	run_sql "CREATE table test.simple0(id int primary key, val int);"
	run_sql "CREATE table test.\`simple-dash\`(id int primary key, val int);"
	run_sql "CREATE table test.simple1(id int primary key, val int);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
		--ssl-ca=$TLS_DIR/ca.pem \
		--ssl-cert=$TLS_DIR/server.pem \
		--ssl-key=$TLS_DIR/server-key.pem
	run_sql "CREATE table test.simple2(id int primary key, val int);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
		--ssl-ca=$TLS_DIR/ca.pem \
		--ssl-cert=$TLS_DIR/server.pem \
		--ssl-key=$TLS_DIR/server-key.pem
	run_sql "INSERT INTO test.simple1(id, val) VALUES (1, 1);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
		--ssl-ca=$TLS_DIR/ca.pem \
		--ssl-cert=$TLS_DIR/server.pem \
		--ssl-key=$TLS_DIR/server-key.pem
	run_sql "INSERT INTO test.simple1(id, val) VALUES (2, 2);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
		--ssl-ca=$TLS_DIR/ca.pem \
		--ssl-cert=$TLS_DIR/server.pem \
		--ssl-key=$TLS_DIR/server-key.pem
	# wait for above sql done in the up source
	sleep 2

	check_table_exists test.simple1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	sequential_cases=(
		"list_changefeed"
		"get_changefeed"
		"pause_changefeed"
		"update_changefeed"
		"resume_changefeed"
		"rebalance_table"
		"list_processor"
		"get_processor"
		"move_table"
		"set_log_level"
		"remove_changefeed"
		"resign_owner"
	)

	for case in ${sequential_cases[@]}; do
		python3 $CUR/util/test_case.py "$case" $TLS_DIR
	done

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
