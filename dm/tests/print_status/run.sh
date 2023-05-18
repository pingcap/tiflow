#!/bin/bash

set -eux

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	run_sql_file $cur/data/db.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# in load stage, the dumped file split into 14 insert segments, we slow down 14 * 100 ms
	# in sync stage, there are 92 group of binlog events, including an XIDEvent,
	# TableMapEvent, QueryEvent, GTIDEvent, and a specific Event in each group.
	# so we slow down 460 * 4 ms. Besides the log may be not flushed to disk asap,
	# we need to add some retry mechanism
	inject_points=("github.com/pingcap/tiflow/dm/worker/PrintStatusCheckSeconds=return(1)"
		"github.com/pingcap/tiflow/dm/loader/LoadDataSlowDown=sleep(100)"
		"github.com/pingcap/tiflow/dm/syncer/ProcessBinlogSlowDown=sleep(4)")
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	cp $cur/conf/dm-worker1.toml $WORK_DIR/dm-worker1.toml
	sed -i "s%placeholder%$WORK_DIR/relay_by_worker%g" $WORK_DIR/dm-worker1.toml

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# start DM task only
	dmctl_start_task_standalone

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	ls $WORK_DIR/relay_by_worker/worker1/*

	run_sql_file $cur/data/db.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	check_log_contains $WORK_DIR/worker1/log/dm-worker.log 'enable safe-mode because of task initialization.*duration=1m0s'
}

function check_print_status() {
	# wait for all dm-worker's log flushed to disk
	i=0
	while [ $i -lt 3 ]; do
		exit_log=$(grep "dm-worker exit" $WORK_DIR/worker1/log/dm-worker.log || echo "not found")
		if [ "$exit_log" == "not found" ]; then
			echo "wait for dm-worker exit log for the $i-th time"
			sleep 1
		else
			break
		fi
	done
	if [ $i -ge 3 ]; then
		echo "wait for dm-worker exit log timeout"
		exit 1
	fi

	echo "checking print status"
	# check dump unit print status
	dump_status_file=$WORK_DIR/worker1/log/dump_status.log
	grep -o "progress status of dumpling" $WORK_DIR/worker1/log/dm-worker.log >$dump_status_file
	dump_status_count=$(wc -l $dump_status_file | awk '{print $1}')
	[ $dump_status_count -ge 1 ]
	# bps must not be zero
	grep -o '\[bps=0' $WORK_DIR/worker1/log/dm-worker.log >$dump_status_file || true
	dump_status_count=$(wc -l $dump_status_file | awk '{print $1}')
	[ $dump_status_count -eq 0 ]

	# check load unit print status
	status_file=$WORK_DIR/worker1/log/loader_status.log
	grep -oP "\[unit=lightning-load\] \[IsCanceled=false\] \[finished_bytes=59674\] \[total_bytes=59674\] \[progress=.*\]" $WORK_DIR/worker1/log/dm-worker.log >$status_file
	status_count=$(wc -l $status_file | awk '{print $1}')
	[ $status_count -eq 1 ]
	# must have a non-zero speed in log
	grep 'current speed (bytes / seconds)' $WORK_DIR/worker1/log/dm-worker.log | grep -vq '"current speed (bytes / seconds)"=0'
	echo "check load unit print status success"

	# check sync unit print status
	status_file2=$WORK_DIR/worker1/log/syncer_status.log
	#grep -oP "syncer.*\Ktotal events = [0-9]+, total tps = [0-9]+, recent tps = [0-9]+, master-binlog = .*" $WORK_DIR/worker1/log/dm-worker.log > $status_file2
	grep -oP "\[total_rows=[0-9]+\] \[total_rps=[0-9]+\] \[rps=[0-9]+\] \[master_position=.*\]" $WORK_DIR/worker1/log/dm-worker.log >$status_file2
	status_count2=$(wc -l $status_file2 | awk '{print $1}')
	[ $status_count2 -ge 1 ]
	echo "check sync unit print status success"
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

check_print_status $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
