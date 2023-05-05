#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

function prepare() {
	if [ "$SINK_TYPE" == "kafka" ]; then
		echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
		exit 0
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
}

function resume_changefeed_in_stopped_state() {
	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	checkpointTs1=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	for i in $(seq 1 2); do
		stmt="create table test.table$i (id int primary key auto_increment, time datetime DEFAULT CURRENT_TIMESTAMP)"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		stmt="insert into test.table$i values (),(),(),(),()"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	for i in $(seq 1 2); do
		table="test.table$i"
		check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config2.toml
	cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr

	checkpointTs2=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	for i in $(seq 3 4); do
		stmt="CREATE table test.table$i (id int primary key auto_increment, time datetime DEFAULT CURRENT_TIMESTAMP)"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		stmt="insert into test.table$i values (),(),(),(),()"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	checkpointTs3=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	for i in $(seq 5 6); do
		stmt="CREATE table test.table$i (id int primary key auto_increment, time datetime DEFAULT CURRENT_TIMESTAMP)"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		stmt="insert into test.table$i values (),(),(),(),()"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	echo "Resume changefeed with checkpointTs3 $checkpointTs3"
	cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr --overwrite-checkpoint-ts=$checkpointTs3 --no-confirm=true
	for i in $(seq 5 6); do
		table="test.table$i"
		check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done

	stmt="select count(*) as table_count from information_schema.tables where table_schema='test'"
	run_sql "$stmt" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains "table_count: 4"
	run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT} && check_contains "table_count: 6"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config1.toml

	cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
	echo "Resume changefeed with checkpointTs1 $checkpointTs1"
	cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr --overwrite-checkpoint-ts=$checkpointTs1 --no-confirm=true
	for i in $(seq 3 4); do
		table="test.table$i"
		check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	run_sql "$stmt" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains "table_count: 6"
	run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT} && check_contains "table_count: 6"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config2.toml

	cleanup_process $CDC_BINARY
}

function resume_changefeed_in_failed_state() {
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/InjectChangefeedFastFailError=1*return(true)'
	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "failed" "ErrStartTsBeforeGC" ""

	cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr --overwrite-checkpoint-ts=now --no-confirm=true
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	run_sql "create database test1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for i in $(seq 1 2); do
		stmt="create table test1.table$i (id int primary key auto_increment, time datetime DEFAULT CURRENT_TIMESTAMP)"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		stmt="insert into test1.table$i values (),(),(),(),()"
		run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	for i in $(seq 1 2); do
		table="test1.table$i"
		check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config3.toml

	cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
	result=$(cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr --overwrite-checkpoint-ts=18446744073709551615 --no-confirm=true 2>&1 || true)
	if [[ $result != *"ErrCliCheckpointTsIsInFuture"* ]]; then
		echo "changefeeed resume result is expected to contain 'ErrCliCheckpointTsIsInFuture', \
          but actually got $result"
		exit 1
	fi

	result=$(cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr --overwrite-checkpoint-ts=100 --no-confirm=true 2>&1 || true)
	if [[ $result != *"ErrStartTsBeforeGC"* ]]; then
		echo "changefeeed resume result is expected to contain 'ErrStartTsBeforeGC', \
			    but actually got $result"
		exit 1
	fi

	gc_safepoint=$(pd-ctl -u=$pd_addr service-gc-safepoint | grep -oE "\"safe_point\": [0-9]+" | grep -oE "[0-9]+" | sort | head -n1)
	result=$(cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr --overwrite-checkpoint-ts=$gc_safepoint --no-confirm=true 2>&1 || true)
	if [[ $result != *"ErrStartTsBeforeGC"* ]]; then
		echo "changefeeed resume result is expected to contain 'ErrStartTsBeforeGC', \
			    but actually got $result"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
prepare
resume_changefeed_in_stopped_state $*
resume_changefeed_in_failed_state $*

check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
