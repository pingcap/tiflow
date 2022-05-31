#!/bin/bash

set -eu

shardddl="shardddl"
shardddl1="shardddl1"
shardddl2="shardddl2"
tb1="tb1"
tb2="tb2"
tb3="tb3"
tb4="tb4"
tb="tb"
t_1="t_1"

function init_database() {
	run_sql_both_source "drop database if exists ${shardddl1};"
	run_sql_both_source "drop database if exists ${shardddl2};"
	run_sql_both_source "create database if not exists ${shardddl1};"
	run_sql_both_source "create database if not exists ${shardddl2};"
}

function extract() {
	str="$1"
	source=${str:0:1}
	database=${str:1:1}
	table=${str:2:1}
}

function init_table() {
	for i in $@; do
		extract $i
		run_sql_source${source} "create table shardddl${database}.tb${table} (id int primary key);"
	done
}

function clean_table() {
	run_sql_both_source "drop table if exists ${shardddl1}.${tb1};"
	run_sql_both_source "drop table if exists ${shardddl1}.${tb2};"
	run_sql_both_source "drop table if exists ${shardddl1}.${tb3};"
	run_sql_both_source "drop table if exists ${shardddl1}.${tb4};"
	run_sql_both_source "drop table if exists ${shardddl2}.${tb1};"
	run_sql_both_source "drop table if exists ${shardddl2}.${tb2};"
	run_sql_both_source "drop table if exists ${shardddl2}.${tb3};"
	run_sql_both_source "drop table if exists ${shardddl2}.${tb4};"
	run_sql_both_source "drop table if exists ${shardddl1}.${t_1};"
	run_sql_both_source "drop table if exists ${shardddl2}.${t_1};"
	run_sql_tidb "drop table if exists ${shardddl}.${tb};"
	run_sql_tidb "drop database if exists dm_meta;"
	run_sql_tidb "drop table if exists ${shardddl}.${t_1};"
}

function restart_master() {
	echo "restart dm-master"
	kill_process dm-master
	check_master_port_offline 1
	sleep 2

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
}

function restart_worker1() {
	echo "restart dm-worker1"
	kill_process worker1
	check_process_exit worker1 20
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
}

function restart_worker2() {
	echo "restart dm-worker2"
	kill_process worker2
	check_process_exit worker2 20
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
}

function restart_task() {
	echo "restart task"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $1"

	if [[ "$task_conf" == *"single"* ]]; then
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"unit\": \"Sync\"" 1
	elif [[ "$task_conf" == *"double"* ]]; then
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"unit\": \"Sync\"" 2
	fi
}

function random_restart() {
	modN=4
	if [ $# -ge 1 ]; then
		modN=$1
	fi
	mod=$(($RANDOM % $modN))
	if [[ "$mod" == "0" ]]; then
		echo "restart master"
		restart_master
	elif [[ "$mod" == "1" ]]; then
		echo "restart worker1"
		restart_worker1
	elif [[ "$mod" == "2" ]]; then
		echo "restart worker2"
		restart_worker2
	else
		echo "restart task"
		restart_task $cur/conf/double-source-optimistic.yaml
	fi
}
