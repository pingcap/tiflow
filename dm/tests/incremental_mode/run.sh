#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"

API_VERSION="v1alpha1"

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	export GO_FAILPOINTS="github.com/pingcap/dm/dm/worker/defaultKeepAliveTTL=return(1)"

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# test keepalive is changed by failpoint, so after 1 second DM master will know not alive
	killall -9 dm-worker.test
	sleep 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"\"stage\": \"offline\"" 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	worker1bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker1 |
		grep 'source' | awk -F: '{print $2}' | cut -d'"' -f 2)
	worker2bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker2 |
		grep 'source' | awk -F: '{print $2}' | cut -d'"' -f 2)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker1bound worker1" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker2bound worker2" \
		"\"result\": true" 1

	# relay should be started after start-relay
	sleep 2
	# and now default keepalive TTL is 30 minutes
	killall -9 dm-worker.test
	sleep 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"\"stage\": \"bound\"" 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# start a task in `full` mode
	echo "start task in full mode"
	cat $cur/conf/dm-task.yaml >$WORK_DIR/dm-task.yaml
	sed -i "s/task-mode-placeholder/full/g" $WORK_DIR/dm-task.yaml
	# avoid cannot unmarshal !!str `binlog-...` into uint32 error
	sed -i "s/binlog-pos-placeholder-1/4/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-2/4/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	dmctl_stop_task $TASK_NAME

	# $worker1_run_source_1 > 0 means source1 is operated to worker1
	worker1_run_source_1=$(sed "s/$SOURCE_ID1/$SOURCE_ID1\n/g" $WORK_DIR/worker1/log/dm-worker.log | grep -c "$SOURCE_ID1") || true
	if [ $worker1_run_source_1 -gt 0 ]; then
		name1=$(grep "Log: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		pos1=$(grep "Pos: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		gtid1=$(grep "GTID:" $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2,":",$3}' | tr -d ' ')
		name2=$(grep "Log: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		pos2=$(grep "Pos: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		gtid2=$(grep "GTID:" $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2,":",$3}' | tr -d ' ')
	else
		name2=$(grep "Log: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		pos2=$(grep "Pos: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		gtid2=$(grep "GTID:" $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2,":",$3}' | tr -d ' ')
		name1=$(grep "Log: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		pos1=$(grep "Pos: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2}' | tr -d ' ')
		gtid1=$(grep "GTID:" $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata | awk -F: '{print $2,":",$3}' | tr -d ' ')
	fi
	# kill worker1 and worker2
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# start a task in `incremental` mode

	# using account with limited privileges
	run_sql_file $cur/data/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_count 'Query OK, 0 rows affected' 7
	run_sql_file $cur/data/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_count 'Query OK, 0 rows affected' 7

	# update mysql config
	sed -i "s/root/dm_incremental/g" $WORK_DIR/source1.yaml
	sed -i "s/relay-binlog-gtid: ''/relay-binlog-gtid: '$gtid1'/g" $WORK_DIR/source1.yaml
	sed -i "s/root/dm_incremental/g" $WORK_DIR/source2.yaml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source update $WORK_DIR/source1.yaml" \
		"Update worker config is not supported by dm-ha now" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source update $WORK_DIR/source2.yaml" \
		"Update worker config is not supported by dm-ha now" 1
	# update mysql config is not supported by dm-ha now, so we stop and start source again to update source config

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $worker1bound worker1" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $worker2bound worker2" \
		"\"result\": true" 1

	dmctl_operate_source stop $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source stop $WORK_DIR/source2.yaml $SOURCE_ID2
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker1bound worker1" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker2bound worker2" \
		"\"result\": true" 1

	echo "start task in incremental mode"
	cat $cur/conf/dm-task.yaml >$WORK_DIR/dm-task.yaml
	sed -i "s/task-mode-placeholder/incremental/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-name-placeholder-1//g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-1//g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-1/$gtid1/g" $WORK_DIR/dm-task.yaml

	sed -i "s/binlog-name-placeholder-2/$name2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-2/$pos2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-2/$gtid2/g" $WORK_DIR/dm-task.yaml

	# test graceful display error
	export GO_FAILPOINTS='github.com/pingcap/dm/syncer/GetEventError=return'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --remove-meta"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"ErrCode\": 36069" 2

	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# only mock pull binlog failed once
	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/WaitUserCancel=return(8);github.com/pingcap/dm/syncer/GetEventError=1*return"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	sleep 3
	# check not specify binlog name could also update active relay log
	if [ $worker1_run_source_1 -gt 0 ]; then
		grep -E ".*current earliest active relay log.*$name1" $WORK_DIR/worker1/log/dm-worker.log
	else
		grep -E ".*current earliest active relay log.*$name1" $WORK_DIR/worker2/log/dm-worker.log
	fi

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Running" 4
	# check reset binlog puller success
	grep -Fq "reset replication binlog puller" $WORK_DIR/worker1/log/dm-worker.log
	grep -Fq "reset replication binlog puller" $WORK_DIR/worker2/log/dm-worker.log
	# we use failpoint to let worker sleep 8 second when executeSQLs, to increase possibility of
	# meeting an error of context cancel.
	# when below check pass, it means we filter out that error, or that error doesn't happen.
	# we only focus on fails, to find any unfiltered context cancel error.
	# and should not contain errors like:
	#   - `driver: bad connection`
	#   - `sql: connection is already closed`
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test" \
		"\"result\": true" 3

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/FlushCheckpointStage=return(100)" # for all stages
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	sleep 3
	dmctl_start_task $WORK_DIR/dm-task.yaml

	# the task should paused by `FlushCheckpointStage` failpont before flush old checkpoint.
	# `db2.increment.sql` has no DDL, so we check count of content as `1`.
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before flush old checkpoint" 1

	# resume-task to next stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before track DDL" 1

	# resume-task to next stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before execute DDL" 1

	# resume-task to next stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before save checkpoint" 1

	# resume-task to next stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before flush checkpoint" 1

	# resume-task to continue the sync
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# test rotate binlog, after rotate and ddl, master binlog should be equal to sync binlog
	run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "truncate table incremental_mode.t1;" $MYSQL_PORT1 $MYSQL_PASSWORD1

	sleep 2
	curl -X GET 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test >$WORK_DIR/status.log
	SYNCER_BINLOG=$(cat $WORK_DIR/status.log | sed 's/.*mysql-replica-01.*\"syncerBinlog\":\"\(.*\)\",\"syncerBinlogGtid.*mysql-replica-02.*/\1/g')
	MASTER_BINLOG=$(cat $WORK_DIR/status.log | sed 's/.*mysql-replica-01.*\"masterBinlog\":\"\(.*\)\",\"masterBinlogGtid.*mysql-replica-02.*/\1/g')

	if [ "$MASTER_BINLOG" != "$SYNCER_BINLOG" ]; then
		echo "master binlog is not equal to syncer binlog"
		cat $WORK_DIR/status.log
		exit 1
	fi

	export GO_FAILPOINTS=''
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
