#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PATH=$CUR/../_utils:$PATH # for sync_diff_inspector

source $CUR/lib.sh

function clean_data() {
	echo "-------clean_data--------"

	exec_sql $master_57_host "stop slave;"
	exec_sql $slave_57_host "stop slave;"
	exec_sql $master_8_host "stop slave;"
	exec_sql $slave_8_host "stop slave;"

	exec_sql $master_57_host "drop database if exists db1;"
	exec_sql $master_57_host "drop database if exists db2;"
	exec_sql $master_57_host "drop database if exists ${db};"
	exec_sql $master_57_host "reset master;"
	exec_sql $slave_57_host "drop database if exists db1;"
	exec_sql $slave_57_host "drop database if exists db2;"
	exec_sql $slave_57_host "drop database if exists ${db};"
	exec_sql $slave_57_host "reset master;"
	exec_sql $master_8_host "drop database if exists db1;"
	exec_sql $master_8_host "drop database if exists db2;"
	exec_sql $master_8_host "drop database if exists ${db};"
	exec_sql $master_8_host "reset master;"
	exec_sql $slave_8_host "drop database if exists db1;"
	exec_sql $slave_8_host "drop database if exists db2;"
	exec_sql $slave_8_host "drop database if exists ${db};"
	exec_sql $slave_8_host "reset master;"
	exec_tidb $tidb_host "drop database if exists db1;"
	exec_tidb $tidb_host "drop database if exists db2;"
	exec_tidb $tidb_host "drop database if exists ${db};"
	rm -rf /tmp/dm_test
}

function cleanup_process() {
	echo "-------cleanup_process--------"
	pkill -hup dm-worker.test 2>/dev/null || true
	pkill -hup dm-master.test 2>/dev/null || true
	pkill -hup dm-syncer.test 2>/dev/null || true

}

function prepare_binlogs() {
	echo "-------prepare_binlogs--------"

	prepare_more_binlogs $master_57_host
	prepare_less_binlogs $slave_57_host
	prepare_less_binlogs $master_8_host
	prepare_more_binlogs $slave_8_host
}

function setup_replica() {
	echo "-------setup_replica--------"

	master_57_status=($(get_master_status $master_57_host))
	slave_57_status=($(get_master_status $slave_57_host))
	master_8_status=($(get_master_status $master_8_host))
	slave_8_status=($(get_master_status $slave_8_host))
	echo "master_57_status" ${master_57_status[@]}
	echo "slave_57_status" ${slave_57_status[@]}
	echo "master_8_status" ${master_8_status[@]}
	echo "slave_8_status" ${slave_8_status[@]}

	# master <-- slave
	change_master_to_pos $slave_57_host $master_57_host ${master_57_status[0]} ${master_57_status[1]}

	# master <--> master
	change_master_to_gtid $slave_8_host $master_8_host
	change_master_to_gtid $master_8_host $slave_8_host
}

function run_dm_components_and_create_sources() {
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
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $CUR/conf/source1.yaml" \
		"\"result\": true" 2

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $CUR/conf/dm-worker2.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"free" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $CUR/conf/source2.yaml" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"alive" 1 \
		"bound" 2
}

function gen_full_data() {
	echo "-------gen_full_data--------"

	exec_sql $host1 "create database ${db};"
	exec_sql $host1 "create table ${db}.${tb}(id int primary key, a int);"
	for i in $(seq 1 100); do
		exec_sql $host1 "insert into ${db}.${tb} values($i,$i);"
	done

	exec_sql $host2 "create database ${db};"
	exec_sql $host2 "create table ${db}.${tb}(id int primary key, a int);"
	for i in $(seq 101 200); do
		exec_sql $host2 "insert into ${db}.${tb} values($i,$i);"
	done
}

function gen_incr_data() {
	echo "-------gen_incr_data--------"

	for i in $(seq 201 250); do
		exec_sql $host1 "insert into ${db}.${tb} values($i,$i);"
	done
	for i in $(seq 251 300); do
		exec_sql $host2 "insert into ${db}.${tb} values($i,$i);"
	done
	exec_sql $host1 "alter table ${db}.${tb} add column b int;"
	exec_sql $host2 "alter table ${db}.${tb} add column b int;"
	for i in $(seq 301 350); do
		exec_sql $host1 "insert into ${db}.${tb} values($i,$i,$i);"
	done
	for i in $(seq 351 400); do
		exec_sql $host2 "insert into ${db}.${tb} values($i,$i,$i);"
	done

	# make master slave switch
	docker-compose -f $CUR/docker-compose.yml pause mysql57_master
	docker-compose -f $CUR/docker-compose.yml pause mysql8_master
	wait_mysql $host1 2
	wait_mysql $host2 2

	for i in $(seq 401 450); do
		exec_sql $host1 "insert into ${db}.${tb} values($i,$i,$i);"
	done
	for i in $(seq 451 500); do
		exec_sql $host2 "insert into ${db}.${tb} values($i,$i,$i);"
	done
	exec_sql $host1 "alter table ${db}.${tb} add column c int;"
	exec_sql $host2 "alter table ${db}.${tb} add column c int;"

	# make sure relay switch before unpause
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status task_pessimistic -s mysql-replica-02" \
		"relaySubDir.*000002" 1

	docker-compose -f $CUR/docker-compose.yml unpause mysql8_master
	wait_mysql $host2 1

	for i in $(seq 501 550); do
		exec_sql $host1 "insert into ${db}.${tb} values($i,$i,$i,$i);"
	done
	for i in $(seq 551 600); do
		exec_sql $host2 "insert into ${db}.${tb} values($i,$i,$i,$i);"
	done
}

function start_task() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $CUR/conf/task-pessimistic.yaml --remove-meta" \
		"\result\": true" 3
}

function verify_result() {
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status task_pessimistic -s mysql-replica-02" \
		"relaySubDir.*000003" 1
}

function clean_task() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task task_pessimistic" \
		"\result\": true" 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s mysql-replica-02" \
		"\result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source stop mysql-replica-01" \
		"\result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source stop mysql-replica-02" \
		"\result\": true" 2
}

function check_master() {
	wait_mysql $host1 1
	wait_mysql $host2 1
}

function test_relay() {
	cleanup_process
	check_master
	install_sync_diff
	clean_data
	prepare_binlogs
	setup_replica
	gen_full_data
	run_dm_components_and_create_sources
	start_task
	gen_incr_data
	verify_result
	clean_task
	docker-compose -f $CUR/docker-compose.yml unpause mysql57_master
	echo "CASE=test_relay success"
}

function test_binlog_filename_change_when_enable_gtid() {
	cleanup_process
	check_master
	clean_data
	setup_replica
	gen_full_data
	run_dm_components_and_create_sources
	start_task

	# flush mysql8 master binlog files -> mysql-bin.000004
	exec_sql $host2 "flush logs;"
	exec_sql $host2 "flush logs;"
	exec_sql $host2 "flush logs;"

	# make mysql8 master slave switch
	docker-compose -f $CUR/docker-compose.yml pause mysql8_master
	wait_mysql $host2 2

	# make a flush job to dm
	exec_sql $host1 "alter table ${db}.${tb} add column c int;"
	exec_sql $host2 "alter table ${db}.${tb} add column c int;"

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	# mysql8 new master's binlog file still is mysql-bin.000001
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status task_pessimistic " \
		"mysql-bin.000001" 5 # source-1 have match 2, source-2 match 3 (relay 2 + task's masterBinlog 1)

	docker-compose -f $CUR/docker-compose.yml unpause mysql8_master
	clean_task
	echo "CASE=test_binlog_filename_change_when_enable_gtid success"
}

function test_binlog_filename_change_when_enable_relay_and_gtid() {
	cleanup_process
	check_master
	clean_data
	setup_replica
	gen_full_data
	run_dm_components_and_create_sources
	start_task

	# flush mysql8 master binlog files -> mysql-bin.000004
	exec_sql $host2 "flush logs;"
	exec_sql $host2 "flush logs;"
	exec_sql $host2 "flush logs;"

	# make mysql8 master slave switch
	docker-compose -f $CUR/docker-compose.yml pause mysql8_master
	wait_mysql $host2 2

	# make a flush job to dm
	exec_sql $host1 "alter table ${db}.${tb} add column c int;"
	exec_sql $host2 "alter table ${db}.${tb} add column c int;"

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	# mysql8 new master's binlog file still is mysql-bin.000001
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status task_pessimistic " \
		"mysql-bin.000001" 5 # source-1 have match 2, source-2 match 3 (relay 2 + task's masterBinlog 1)

	docker-compose -f $CUR/docker-compose.yml unpause mysql8_master
	clean_task
	echo "CASE=test_binlog_filename_change_when_enable_relay_and_gtid success"
}

function run() {
	test_relay
	test_binlog_filename_change_when_enable_gtid
	test_binlog_filename_change_when_enable_relay_and_gtid
}

run
