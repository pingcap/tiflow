#!/bin/bash

function handle_error_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error" \
		"handle-error <task-name | task-file> \[-s source ...\] \[-b binlog-pos\] <skip\/replace\/revert> \[replace-sql1;replace-sql2;\] \[flags\]" 1
}

function handle_error_invalid_binlogpos() {
	binlog_pos="mysql-bin:shoud-bin-digital"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test-task --binlog-pos $binlog_pos skip" \
		"\[.*\], Message: invalid --binlog-pos $binlog_pos in handle-error operation: the pos should be digital" 1
}

function handle_error_invalid_sqls() {
	sqls="alter table tb add column a int; alter table tb2 b int;"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test-task replace $sqls" \
		"invalid sql" 1
}

function handle_error_invalid_op() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test-task wrong_operation" \
		"invalid operation 'wrong_operation'" 1
}
