#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_CONF=$cur/conf/dm-task.yaml
TASK_NAME="test"
WORKER1_CONF=$cur/conf/dm-worker1.toml
SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

# used to coverage wrong usage of dmctl command
function usage_and_arg_test() {
	#    migrate_relay_wrong_arg
	#    migrate_relay_without_worker

	#    switch_relay_master_wrong_arg
	#    switch_relay_master_without_worker

	unlock_ddl_lock_wrong_arg
	unlock_ddl_lock_invalid_force_remove

	handle_error_wrong_arg
	handle_error_invalid_binlogpos
	handle_error_invalid_sqls
	handle_error_invalid_op

	binlog_empty_arg
	binlog_invalid_binlogpos
	binlog_invalid_sqls_by_replace
	binlog_invalid_sqls_by_inject
	binlog_invalid_list_op
	binlog_invalid_op

	source_table_schema_empty_arg
	source_table_schema_lack_arguments

	# TODO: check SQLs error test
}

function run() {
	cd $cur
	for file in "check_list"/*; do
		source $file
	done
	cd -

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	usage_and_arg_test
}

cleanup_data dmctl_advance
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
