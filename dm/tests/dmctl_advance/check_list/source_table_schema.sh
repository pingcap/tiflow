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
		"binlog-schema update test" \
		"dmctl binlog-schema update <task-name> <table-filter1> <table-filter2> ... <schema-file> \[flags\]" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema delete test" \
		"delete <task-name> <table-filter1> <table-filter2> ... \[flags\]" 1
}
