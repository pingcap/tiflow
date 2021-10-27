#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
db="fake_rotate_event"
tb="t1"

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_start_task_standalone "$cur/conf/dm-task.yaml" "--remove-meta"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		'"synced": true' 1

	# make binlog rotate
	run_sql_source1 "flush logs;"
	run_sql_source1 "use $db; insert into $tb values (3, 3, 3)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "kill dm-worker"
	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20

	# make fake rotate event and rewrite binlog filename to mysql-bin.000001
	export GO_FAILPOINTS='github.com/pingcap/dm/syncer/MakeFakeRotateEvent=return("mysql-bin.000001")'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# make a fake rotae event
	run_sql_source1 "flush logs;"
	run_sql_source1 "use $db;alter table $tb add column info2 varchar(40);" # trigger a flush job
	run_sql_source1 "use $db; insert into $tb values (4, 4, 4,'info')"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# check syncer's binlog filename same with fake rotate file name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"mysql-bin.000001" 1

	export GO_FAILPOINTS=''
}

cleanup_data fake_rotate_event
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
