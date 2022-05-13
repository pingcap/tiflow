#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"

API_VERSION="v1alpha1"

function get_uuid() {
	uuid=$(echo "show variables like '%server_uuid%';" | MYSQL_PWD=123456 mysql -uroot -h$1 -P$2 | awk 'FNR == 2 {print $2}')
	echo $uuid
}

function get_binlog_name() {
	binlog_name=$(echo "SHOW BINARY LOGS;" | MYSQL_PWD=123456 mysql -uroot -h127.0.0.1 -P3307 | awk 'FNR == 2 {print $1}')
	echo $binlog_name
}

######################################################
#   		this test also used by binlog 999999 test
######################################################
function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'
	uuid=($(get_uuid $MYSQL_HOST1 $MYSQL_PORT1))
	binlog_name=($(get_binlog_name $MYSQL_HOST2 $MYSQL_PORT2))

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/dm/worker/defaultKeepAliveTTL=return(1)"

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
	sed -i "s/binlog-gtid-placeholder/$uuid:0/g" $WORK_DIR/source1.yaml
	sed -i "s/binlog-name-placeholder/$binlog_name/g" $WORK_DIR/source2.yaml
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
		"\"result\": true" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker2bound worker2" \
		"\"result\": true" 2

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

	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# using account with limited privileges
	run_sql_file $cur/data/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_count 'Query OK, 0 rows affected' 7
	run_sql_file $cur/data/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_count 'Query OK, 0 rows affected' 7

	# update mysql config
	sed -i "s/root/dm_incremental/g" $WORK_DIR/source1.yaml
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
		"\"result\": true" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $worker2bound worker2" \
		"\"result\": true" 2

	dmctl_operate_source stop $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source stop $WORK_DIR/source2.yaml $SOURCE_ID2
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker1bound worker1" \
		"\"result\": true" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker2bound worker2" \
		"\"result\": true" 2

	worker1_run_source_1=$(sed "s/$SOURCE_ID1/$SOURCE_ID1\n/g" $WORK_DIR/worker1/log/dm-worker.log | grep -c "$SOURCE_ID1") || true
	echo "start task in incremental mode with zero gtid/pos"
	sed "s/binlog-gtid-placeholder-1/$uuid:0/g" $cur/conf/dm-task.yaml >$WORK_DIR/dm-task.yaml
	sed -i "s/binlog-name-placeholder-2/$binlog_name/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-2/4/g" $WORK_DIR/dm-task.yaml

	# test graceful display error
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/GetEventError=return'
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
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/WaitUserCancel=return(8);github.com/pingcap/tiflow/dm/syncer/GetEventError=1*return"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	sleep 3
	# check not specify binlog name could also update active relay log
	if [ $worker1_run_source_1 -gt 0 ]; then
		grep -E ".*current earliest active relay log.*$binlog_name" $WORK_DIR/worker2/log/dm-worker.log
	else
		grep -E ".*current earliest active relay log.*$binlog_name" $WORK_DIR/worker1/log/dm-worker.log
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

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/FlushCheckpointStage=return(100)" # for all stages
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	sleep 3
	# start DM task. don't check error because it will meet injected error soon
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml"

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
