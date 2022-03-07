#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DATA_DIR=$cur/../load_task/data
CONF_DIR=$cur/../load_task/conf
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
WORKER1="worker1"
WORKER2="worker2"
WORKER3="worker3"

function test_worker_restart() {
	echo "test worker restart"
	# worker1 offline
	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20

	# source1 bound to worker3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker3" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-01\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker1" \
		"\"stage\": \"offline\"" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task1" \
		"different worker in load stage, previous worker: $WORKER1, current worker: $WORKER3" 1 \
		"Please check if the previous worker is online." 1

	# worker1 online
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/LoadDataSlowDownByTask=return(\"load_task1\")"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $CONF_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# transfer to worker1
	check_log_contain_with_retry 'transfer source and worker.*worker1.*worker3.*mysql-replica-01' $WORK_DIR/master/log/dm-master.log
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker3" \
		"\"stage\": \"free\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker1" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-01\"" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task1" \
		"\"unit\": \"Load\"" 1 \
		"\"unit\": \"Sync\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task2" \
		"\"unit\": \"Load\"" 1 \
		"\"unit\": \"Sync\"" 1
}

# almost never happen since user hardly start a load task after another load task failed.
function test_transfer_two_sources() {
	echo "test_transfer_two_sources"
	# worker2 offline
	ps aux | grep dm-worker2 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER2_PORT 20

	# source2 bound to worker3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker3" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-02\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task2" \
		"different worker in load stage, previous worker: $WORKER2, current worker: $WORKER3" 1

	# start load task for worker3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task3.yaml --remove-meta" \
		"\"result\": true" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task3" \
		"\"unit\": \"Load\"" 1

	# worker2 online
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/LoadDataSlowDown=sleep(15000)"
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $CONF_DIR/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# worker2 free since (worker3, source2) has load task(load_task3)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker2" \
		"\"stage\": \"free\"" 1

	# worker1 offline
	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20

	# source1 bound to worker2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker2" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-01\"" 1

	# start load_task4 on worker2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task4.yaml --remove-meta" \
		"\"result\": true" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task4" \
		"\"unit\": \"Load\"" 1

	# worker1 online
	export GO_FAILPOINTS=""
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $CONF_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# worker1 free since (worker2, source1) has load task(load_task4)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker1" \
		"\"stage\": \"free\"" 1

	# now, worker2 waiting worker3 finish load_task3, worker1 waiting worker2 finish load_task4
	# worker3 offline
	ps aux | grep dm-worker3 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER3_PORT 20

	# source2 bound to worker1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker1" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-02\"" 1

	# (worker1, source2), (worker2, source1)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task1" \
		"different worker in load stage, previous worker: $WORKER1, current worker: $WORKER2" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task2" \
		"different worker in load stage, previous worker: $WORKER2, current worker: $WORKER1" 1

	# worker2 finish load_task4
	# master transfer (worker1, source2), (worker2, source1) to (worker1, source1), (worker2, source2)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker1" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-01\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker2" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-02\"" 1

	# task1, 2, 4 running, task3 fail
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status" \
		"\"taskStatus\": \"Running\"" 3 \
		"taskStatus.*Error" 1

	# worker3 online
	export GO_FAILPOINTS=""
	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $CONF_DIR/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	# source2 bound to worker3 since load_task3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w -n worker2" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-02\"" 1

	# all task running
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status" \
		"\"taskStatus\": \"Running\"" 4
}

function run() {
	echo "import prepare data"
	run_sql_file $DATA_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $DATA_DIR/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	echo "start DM master, workers and sources"
	run_dm_master $WORK_DIR/master $MASTER_PORT1 $CONF_DIR/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

	# worker1 loading load_task1
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/LoadDataSlowDownByTask=return(\"load_task1\")"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $CONF_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	cp $CONF_DIR/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# worker2 loading load_task2
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/LoadDataSlowDownByTask=return(\"load_task2\")"
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $CONF_DIR/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	cp $CONF_DIR/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# worker3 loading load_task3
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/LoadDataSlowDownByTask=return(\"load_task3\")"
	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $CONF_DIR/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	echo "start DM task"
	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
	dmctl_start_task "$cur/conf/dm-task2.yaml" "--remove-meta"

	check_log_contain_with_retry 'inject failpoint LoadDataSlowDownByTask in lightning loader' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry 'inject failpoint LoadDataSlowDownByTask in lightning loader' $WORK_DIR/worker2/log/dm-worker.log
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task1" \
		"\"unit\": \"Load\"" 1 \
		"\"unit\": \"Sync\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status load_task2" \
		"\"unit\": \"Load\"" 1 \
		"\"unit\": \"Sync\"" 1

	test_worker_restart

	test_transfer_two_sources

	run_sql_file $DATA_DIR/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $DATA_DIR/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_sync_diff $WORK_DIR $CONF_DIR/diff_config1.toml
	check_sync_diff $WORK_DIR $CONF_DIR/diff_config2.toml
	check_sync_diff $WORK_DIR $CONF_DIR/diff_config3.toml
	check_sync_diff $WORK_DIR $CONF_DIR/diff_config4.toml
}

cleanup_data load_task1
cleanup_data load_task2
cleanup_data load_task3
cleanup_data load_task4
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
