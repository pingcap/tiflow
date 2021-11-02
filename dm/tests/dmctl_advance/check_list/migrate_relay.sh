#!/bin/bash

function migrate_relay_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"migrate-relay" \
		"migrate-relay <source> <binlogName> <binlogPos> \[flags\]" 1
}

function migrate_relay_without_worker() {
	binlog_pos="invalid-binlog-pos"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"migrate-relay $SOURCE_ID1 bin-000001 $binlog_pos" \
		"strconv.Atoi: parsing \"$binlog_pos\": invalid syntax" 1
}
