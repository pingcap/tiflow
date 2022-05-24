#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"

function run() {
	echo "import prepare data"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	echo "start DM worker and master"
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2

	# master1 or master2 join campaign
	check_metric $MASTER_PORT2 'start_leader_counter' 3 0 2 || check_metric $MASTER_PORT1 'start_leader_counter' 3 0 2

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	echo "operate mysql config to worker"
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2

	# join master3
	run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $cur/conf/dm-master3.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3
	check_metric $MASTER_PORT3 'start_leader_counter' 3 -1 1 # master3 is not leader

	# worker in running stage
	check_metric $MASTER_PORT1 'dm_master_worker_state{worker="worker1"}' 3 1 3 || check_metric $MASTER_PORT2 'dm_master_worker_state{worker="worker1"}' 3 1 3

	echo "start DM task"
	dmctl_start_task

	echo "use sync_diff_inspector to check full dump loader"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "flush logs to force rotate binlog file"
	run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "apply increment data before restart dm-worker to ensure entering increment phase"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "pause task before kill and restart dm-worker"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test" \
		"\"result\": true" 3

	echo "start dm-worker3 and kill dm-worker2"
	kill_process dm-worker2
	check_port_offline $WORKER2_PORT 20
	rm -rf $WORK_DIR/worker2/relay_log

	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2

	sleep 8
	echo "wait for the task to be scheduled and keep paused"
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Paused"' 10

	echo "resume task before kill and restart dm-worker"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	echo "start dm-worker2 and kill dm-worker3"
	kill_process dm-worker3
	check_port_offline $WORKER3_PORT 20
	rm -rf $WORK_DIR/worker3/relay_log

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	sleep 8
	echo "wait and check task running"
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10

	# manually transfer a exist source to a newly started worker
	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source $SOURCE_ID1 worker3" \
		"\"result\": true" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --name worker3" \
		"$SOURCE_ID1" 1

	echo "query-status from all dm-master"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	echo "join new dm-master and query-status"
	run_dm_master $WORK_DIR/master4 $MASTER_PORT4 $cur/conf/dm-master4.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT4
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT4" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	# may join failed with error `fail to join embed etcd: add member http://127.0.0.1:8295: etcdserver: unhealthy cluster`, and dm-master will exit. so just sleep some seconds.
	sleep 5

	run_dm_master $WORK_DIR/master5 $MASTER_PORT5 $cur/conf/dm-master5.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT5
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT5" \
		"query-status test" \
		"\"stage\": \"Running\"" 3
	sleep 5

	run_dm_master $WORK_DIR/master6 $MASTER_PORT6 $cur/conf/dm-master6.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT6
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT6" \
		"query-status test" \
		"\"stage\": \"Running\"" 3
	sleep 5

	echo "kill dm-master1"
	kill_process dm-master1
	check_master_port_offline 1
	echo "kill dm-master2"
	kill_process dm-master2
	check_master_port_offline 2

	echo "initial cluster of dm-masters have been killed"
	echo "now we will check whether joined masters can work normally"

	# we need some time for cluster to re-elect new available leader
	dmctl_stop_task_with_retry "test" $MASTER_PORT5

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	sleep 2

	# leader needs some time to rebuild info
	# start-task is not retryable
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT5" \
		"start-task $cur/conf/dm-task.yaml" \
		"\"result\": true" 3 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1

	echo "use sync_diff_inspector to check increment2 data now!"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data ha_test
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
