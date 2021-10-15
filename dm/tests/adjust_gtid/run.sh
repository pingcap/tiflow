#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
TASK_NAME="test"
WORK_DIR=$TEST_DIR/$TEST_NAME
# SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

# clean_gtid will
# 1. delete source1's gtid info, but keep the binlog pos info (simulate switch gtid and resume from checkpoint)
# 2. delete source2's checkpoint info, set only binlog pos info in the task.yaml (simulate switch gtid and start for meta)
function clean_gtid() {
	# delete SOURCE1 checkpoint's gtid info
	run_sql "update dm_meta.${TASK_NAME}_syncer_checkpoint set binlog_gtid=\"\" where id=\"$SOURCE_ID1\" and is_global=1" $TIDB_PORT $TIDB_PASSWORD
	# set SOURCE2 incremental metadata without checkpoint
	run_sql "delete from dm_meta.${TASK_NAME}_syncer_checkpoint where id=\"$SOURCE_ID2\" and is_global=1" $TIDB_PORT $TIDB_PASSWORD

	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/task-mode-placeholder/incremental/g" $WORK_DIR/dm-task.yaml
	# avoid cannot unmarshal !!str `binlog-...` into uint32 error
	sed -i "s/binlog-name-placeholder-1/$name1/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-1/4/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-1/\"\"/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-name-placeholder-2/$name2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-2/$pos2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-2/\"\"/g" $WORK_DIR/dm-task.yaml
}

# check_checkpoint checks checkpoint data from the database
function check_checkpoint() {
	source_id=$1
	expected_name=$2
	expected_pos=$3
	expected_gtid=$4

	run_sql "select binlog_name,binlog_pos,binlog_gtid from dm_meta.${TASK_NAME}_syncer_checkpoint where id=\"$source_id\" and is_global=1" $TIDB_PORT $TIDB_PASSWORD
	check_contains $expected_name
	check_contains $expected_pos
	if [[ -n $expected_gtid ]]; then
		check_contains $expected_gtid
	fi
}

function run() {
	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO'"
	run_sql_source1 "SET @@global.time_zone = '+01:00';"
	run_sql_source2 "SET @@global.time_zone = '+02:00';"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	export GO_FAILPOINTS='github.com/pingcap/dm/syncer/AdjustGTIDExit=return(true)'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	# make sure source1 is bound to worker1
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task only
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/task-mode-placeholder/all/g" $WORK_DIR/dm-task.yaml
	# avoid cannot unmarshal !!str `binlog-...` into uint32 error
	sed -i "s/binlog-pos-placeholder-1/4/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-2/4/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	name1=$(grep "Log: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
	pos1=$(grep "Pos: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
	gtid1=$(grep "GTID:" $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2,":",$3}' | tr -d ' ')
	name2=$(grep "Log: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
	pos2=$(grep "Pos: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
	gtid2=$(grep "GTID:" $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2,":",$3}' | tr -d ' ')

	check_checkpoint $SOURCE_ID1 $name1 $pos1 $gtid1
	check_checkpoint $SOURCE_ID2 $name2 $pos2 $gtid2
	dmctl_stop_task_with_retry $TASK_NAME $MASTER_PORT
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20
	clean_gtid

	# start two workers again
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# start task without checking, worker may exit before we get success result
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "start-task $WORK_DIR/dm-task.yaml"

	check_checkpoint $SOURCE_ID1 $name1 $pos1 $gtid1
	check_checkpoint $SOURCE_ID2 $name2 $pos2 $gtid2
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20
	clean_gtid

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	export GO_FAILPOINTS=''
	# start two workers again
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# use sync_diff_inspector to check incremental dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
	run_sql_both_source "SET @@global.time_zone = 'SYSTEM';"
}

cleanup_data adjust_gtid
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
