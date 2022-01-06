#!/bin/bash

function source_table_schema_empty_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema" \
		"Available Commands" 1
}

function source_table_schema_lack_arguments() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema test" \
		"Available Commands" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema list test" \
		"dmctl binlog-schema list <task-name> <database> <table>" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test" \
		"dmctl binlog-schema update <task-name> <database> <table> \[schema-file\] \[flags\]" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test test_db test_table --from-source" \
		"must specify at least one source" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test test_db test_table --from-source --from-target" \
		"from-source and from-target can not be used together" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test test_db test_table schema.sql --from-source" \
		"can not set schema-file when use from-source or from-target" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema delete test" \
		"delete <task-name> <database> <table> \[flags\]" 1
}
