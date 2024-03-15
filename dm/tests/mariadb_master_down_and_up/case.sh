#!/bin/bash

set -exu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PATH=$CUR/../_utils:$PATH # for sync_diff_inspector

source $CUR/lib.sh

function clean_data() {
	echo "-------clean_data--------"

	exec_sql $slave_port "stop slave;"
	exec_sql $slave_port "reset master;"

	exec_sql $master_port "drop database if exists db1;"
	exec_sql $master_port "drop database if exists db2;"
	exec_sql $master_port "drop database if exists ${db};"
	exec_sql $slave_port "drop database if exists db1;"
	exec_sql $slave_port "drop database if exists db2;"
	exec_sql $slave_port "drop database if exists ${db};"
	exec_sql $slave_port "reset master;"
	exec_tidb $tidb_port "drop database if exists db1;"
	exec_tidb $tidb_port "drop database if exists db2;"
	exec_tidb $tidb_port "drop database if exists ${db};"
	rm -rf /tmp/dm_test
}

function cleanup_process() {
	echo "-------cleanup_process--------"
	pkill -hup dm-worker.test 2>/dev/null || true
	pkill -hup dm-master.test 2>/dev/null || true
	pkill -hup dm-syncer.test 2>/dev/null || true
}

function setup_replica() {
	echo "-------setup_replica--------"

	master_status=($(get_master_status))
	master_gtid=$(exec_sql $master_port "select binlog_gtid_pos('${master_status[0]}', ${master_status[1]})" | awk 'NR==2')
	exec_sql $slave_port "set global gtid_slave_pos = '$master_gtid';"

	# master --> slave
	change_master_to_gtid $slave_port $master_port
}

function run_dm_components_and_create_source() {
	echo "-------run_dm_components--------"

	pkill -9 dm-master || true
	pkill -9 dm-worker || true

	run_dm_master $WORK_DIR/master $MASTER_PORT $CUR/conf/dm-master.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"alive" 1

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $CUR/conf/dm-worker1.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"free" 1
	if [ "$1" = "relay" ]; then
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"operate-source create $CUR/conf/source1_relay.yaml" \
			"\"result\": true" 2
	else
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"operate-source create $CUR/conf/source1.yaml" \
			"\"result\": true" 2
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"alive" 1 \
		"bound" 1
}

function gen_full_data() {
	echo "-------gen_full_data--------"

	exec_sql $master_port "create database ${db} collate latin1_bin;"
	exec_sql $master_port "create table ${db}.${tb}(id int primary key, a int);"
	for i in $(seq 1 100); do
		exec_sql $master_port "insert into ${db}.${tb} values($i,$i);"
	done
}

function start_task() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $CUR/conf/task-pessimistic.yaml --remove-meta" \
		"\"result\": true" 2
}

function verify_result() {
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

function clean_task() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task task_pessimistic" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source stop mysql-replica-01" \
		"\"result\": true" 2
}

function test_master_down_and_up() {
	cleanup_process
	clean_data
	install_sync_diff
	setup_replica
	gen_full_data
	run_dm_components_and_create_source $1
	start_task
	verify_result
	echo "-------start test--------"

	for i in $(seq 201 250); do
		exec_sql $master_port "insert into ${db}.${tb} values($i,$i);"
	done
	verify_result

	# make master down
	docker-compose -f $CUR/docker-compose.yml pause mariadb_master
	# execute sqls in slave
	for i in $(seq 401 450); do
		exec_sql $slave_port "insert into ${db}.${tb} values($i,$i);"
	done
	verify_result

	# make master up
	docker-compose -f $CUR/docker-compose.yml unpause mariadb_master
	for i in $(seq 501 550); do
		exec_sql $master_port "insert into ${db}.${tb} values($i,$i);"
	done

	verify_result

	clean_task
	echo "CASE=test_master_down_and_up $1 success"
}

function run() {
	wait_mysql 3306 1
	wait_mysql 3307 2
	test_master_down_and_up no_relay
	test_master_down_and_up relay
}

run
