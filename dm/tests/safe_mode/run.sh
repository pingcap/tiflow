#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function consistency_none() {
	run_sql_source2 "SET @@GLOBAL.SQL_MODE='ANSI_QUOTES'"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "/enable-heartbeat/i\clean-dump-file: false" $WORK_DIR/dm-task.yaml
	sed -i "s/extra-args: \"\"/extra-args: \"--consistency none\"/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	sleep 1
	# make sure dumpling's metadata added empty line after two SHOW MASTER STATUS
	empty_line=$(grep -cvE '\S' $WORK_DIR/worker1/dumped_data.test/metadata)
	if [ $empty_line -ne 2 ]; then
		echo "wrong number of empty line in dumpling's metadata"
		exit 1
	fi
	empty_line=$(grep -cvE '\S' $WORK_DIR/worker2/dumped_data.test/metadata)
	if [ $empty_line -ne 2 ]; then
		echo "wrong number of empty line in dumpling's metadata"
		exit 1
	fi

	name1=$(grep "Log: " $WORK_DIR/worker1/dumped_data.test/metadata | tail -1 | awk -F: '{print $2}' | tr -d ' ')
	pos1=$(grep "Pos: " $WORK_DIR/worker1/dumped_data.test/metadata | tail -1 | awk -F: '{print $2}' | tr -d ' ')
	gtid1=$(grep "GTID:" $WORK_DIR/worker1/dumped_data.test/metadata | tail -1 | awk -F: '{print $2,":",$3}' | tr -d ' ')
	check_log_contain_with_retry "\[\"compare exitPoint and beginLocation\"\] \[task=test\] \[unit=\"binlog replication\"\] \[exitPoint=\"position: ($name1, $pos1), gtid-set: $gtid1\"\]" $WORK_DIR/worker1/log/dm-worker.log
	name2=$(grep "Log: " $WORK_DIR/worker2/dumped_data.test/metadata | tail -1 | awk -F: '{print $2}' | tr -d ' ')
	pos2=$(grep "Pos: " $WORK_DIR/worker2/dumped_data.test/metadata | tail -1 | awk -F: '{print $2}' | tr -d ' ')
	gtid2=$(grep "GTID:" $WORK_DIR/worker2/dumped_data.test/metadata | tail -1 | awk -F: '{print $2,":",$3}' | tr -d ' ')
	check_log_contain_with_retry "\[\"compare exitPoint and beginLocation\"\] \[task=test\] \[unit=\"binlog replication\"\] \[exitPoint=\"position: ($name2, $pos2), gtid-set: $gtid2\"\]" $WORK_DIR/worker2/log/dm-worker.log
	run_sql_source2 "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
	cleanup_process $*
	cleanup_data safe_mode_target
}

function check_exit_safe_binlog() {
	source_id=$1
	compare_gtid=$2
	compare=$3

	bash -c "$cur/../bin/check_exit_safe_binlog $TIDB_PASSWORD $source_id $compare_gtid \"$compare\""
}

function safe_mode_recover() {
	i=1
	while [ $i -lt 5 ]; do
		echo "start to run safe mode case $i"
		export GO_FAILPOINTS=""
		run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
		check_contains 'Query OK, 2 rows affected'
		run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
		check_contains 'Query OK, 3 rows affected'

		run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
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

		dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

		kill_dm_worker

		export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/SafeModeExit=return($i)"
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

		# DM-worker returns error due to the mocked error
		run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
		expected_paused=2
		if [ $i -ge 2 ] && [ $i -le 3 ]; then
			expected_paused=1
		fi
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Paused" $expected_paused
		if [ $expected_paused -eq 1 ]; then
			run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
				"pause-task test" \
				"\"result\": true" 3
			echo 'create table t1 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;' >${WORK_DIR}/schema.sql
			run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
				"operate-schema set -s mysql-replica-01 test -d safe_mode_test -t t1 ${WORK_DIR}/schema.sql" \
				"\"result\": true" 2
			echo 'create table t2 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT = 100;' >${WORK_DIR}/schema.sql
			run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
				"operate-schema set -s mysql-replica-01 test -d safe_mode_test -t t2 ${WORK_DIR}/schema.sql" \
				"\"result\": true" 2
			echo 'create table t2 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT = 200;' >${WORK_DIR}/schema.sql
			run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
				"operate-schema set -s mysql-replica-02 test -d safe_mode_test -t t2 ${WORK_DIR}/schema.sql" \
				"\"result\": true" 2
			echo 'create table t3 (id bigint auto_increment, uid int, name varchar(80), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT = 300;' >${WORK_DIR}/schema.sql
			run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
				"operate-schema set -s mysql-replica-02 test -d safe_mode_test -t t3 ${WORK_DIR}/schema.sql" \
				"\"result\": true" 2
		fi

		kill_dm_worker

		compare="<"
		if [ $i -lt 2 ]; then
			compare="="
		fi
		check_exit_safe_binlog "mysql-replica-01" "true" $compare
		check_exit_safe_binlog "mysql-replica-02" "false" $compare

		export GO_FAILPOINTS=""
		# clean failpoint, should sync successfully
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"resume-task test" \
			"\"result\": true" 3
		sleep 3
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Running" 3
		echo "check sync diff after clean SafeModeExit failpoint"
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

		# DM-worker exit when waiting for sharding group synced
		run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
		echo "check sync diff after restart DDL owner"
		sleep 3
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"pause-task test" \
			"\"result\": true" 3
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Paused" 2

		check_exit_safe_binlog "mysql-replica-01" "true" "="
		check_exit_safe_binlog "mysql-replica-02" "false" "="

		echo "finish running run safe mode recover case $i"
		((i += 1))
		cleanup_process $*
		cleanup_data safe_mode_target
	done
}

function safe_mode_duration() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# make sure exitPoint not equal to beginPosition
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/SkipSaveGlobalPoint=return()"
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	# worker1 -> source1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	# worker2 -> source2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	dmctl_start_task "$cur/conf/dm-task-safe-mode-duration.yaml" "--remove-meta"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# make sure worker1 enter to sync status, and complete one dml
	check_log_contain_with_retry "alter table t1 add column age int" $WORK_DIR/worker1/log/dm-worker.log
	# restart workers
	kill_dm_worker

	export GO_FAILPOINTS=""
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"your \`safe-mode-duration\` in task.yaml is set to 0s" 1

	# stop and start task success
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3

	cp $cur/conf/dm-task-safe-mode-duration.yaml $WORK_DIR/dm-task-safe-mode-duration.yaml
	sed -i "s/safe-mode-duration: \"0s\"/safe-mode-duration: \"30s\"/" $WORK_DIR/dm-task-safe-mode-duration.yaml

	dmctl_start_task "$WORK_DIR/dm-task-safe-mode-duration.yaml"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	sed -i "s/safe-mode-duration: \"30s\"/safe-mode-duration: \"0s\"/" $WORK_DIR/dm-task-safe-mode-duration.yaml
	# stop and start task success
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	dmctl_start_task "$WORK_DIR/dm-task-safe-mode-duration.yaml"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "finish running safe mode duration case"
	cleanup_process $*
	cleanup_data safe_mode_target
}

function run() {
	consistency_none
	safe_mode_recover
	safe_mode_duration

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ReSyncExit=return(true)'
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
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

	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# DM-worker exit during re-sync after sharding group synced
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/ShardSyncedExecutionExit=return(true);github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds=return(\"300s\")"

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	sleep 5
	echo "check sync diff after set SafeModeInitPhaseSeconds failpoint"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# DM-worker exit when waiting for sharding group synced
	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	OWNER_PORT=""
	i=0
	while [ $i -lt 10 ]; do
		# we can't determine which DM-worker is the sharding lock owner, so we try both of them
		# DM-worker1 is sharding lock owner and exits
		if [ "$(check_port_return $WORKER1_PORT)" == "0" ]; then
			echo "DM-worker1 is sharding lock owner and detects it offline"
			export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds=return(\"0s\")"
			run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
			check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
			check_instance_id="1"
			OWNER_PORT=$WORKER1_PORT
			break
		fi
		# DM-worker2 is sharding lock owner and exits
		if [ "$(check_port_return $WORKER2_PORT)" == "0" ]; then
			echo "DM-worker2 is sharding lock owner and detects it offline"
			export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds=return(\"0s\")"
			run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
			check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
			check_instance_id="2"
			OWNER_PORT=$WORKER2_PORT
			break
		fi

		((i += 1))
		echo "wait for one of DM-worker offine failed, retry later" && sleep 1
	done
	if [ $i -ge 10 ]; then
		echo "wait DM-worker offline timeout"
		exit 1
	fi

	sleep 5
	echo "check sync diff after restart DDL owner"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data safe_mode_target
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
