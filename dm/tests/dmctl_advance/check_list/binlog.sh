#!/bin/bash

function binlog_empty_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog" \
		"Available Commands" 1
}

function binlog_invalid_binlogpos() {
	binlog_pos="mysql-bin:should-be-digital"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test-task --binlog-pos $binlog_pos" \
		"\[.*\], Message: invalid --binlog-pos $binlog_pos in handle-error operation: the pos should be digital" 1
}

function binlog_invalid_sqls() {
	sqls="alter table tb add column a int; alter table tb2 b int;"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test-task $sqls" \
		"invalid sql" 1
}

function binlog_invalid_op() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog wrong_operation test-task" \
		"Available Commands" 1
}
