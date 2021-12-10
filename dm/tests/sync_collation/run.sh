#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
db="sync_collation"
db_increment="sync_collation_increment"
db2="sync_collation2"
db_increment2="sync_collation_increment2"
tb="t"
tb2="t2"
tb_check="t_check"

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	echo "start task in full mode"
	cat $cur/conf/dm-task.yaml >$WORK_DIR/dm-task.yaml
	TASK_NAME="full_test"
	sed -i "s/task-name-placeholder/${TASK_NAME}/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml

	# check table
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb} where name = 'aa';" "count(1): 2"

	# check column
	# run_sql_tidb_with_retry "select count(1) from ${db}.${tb2} where name = 'aa';" "count(1): 2"
	# run_sql_tidb_with_retry "select count(1) from ${db2}.${tb2} where name = 'aa';" "count(1): 2"

	# check database by create table
	run_sql_file $cur/data/tidb.checktable.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb_check} where name = 'aa';" "count(1): 2"
	run_sql_file $cur/data/tidb.checktable.prepare2.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb_check} where name = 'aa';" "count(1): 2"

	echo "stop task ${TASK_NAME}"
	dmctl_stop_task $TASK_NAME $MASTER_PORT

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

	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	echo "start task in incremental mode"
	TASK_NAME="incremental_test"
	cat $cur/conf/dm-task-increment.yaml >$WORK_DIR/dm-task.yaml
	sed -i "s/task-name-placeholder/${TASK_NAME}/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-name-placeholder-1/$name1/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-1/$pos1/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-1/$gtid1/g" $WORK_DIR/dm-task.yaml

	sed -i "s/binlog-name-placeholder-2/$name2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-2/$pos2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-2/$gtid2/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# check table
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb} where name = 'aa';" "count(1): 2"

	# check column
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb2} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb2} where name = 'aa';" "count(1): 2"

	# check database by create table
	run_sql_file $cur/data/tidb.checktable.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb_check} where name = 'aa';" "count(1): 2"
	run_sql_file $cur/data/tidb.checktable.prepare2.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb_check} where name = 'aa';" "count(1): 2"

	# check set server collation
	run_sql_tidb_with_retry "select count(1) from ${db_increment}.${tb} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db_increment2}.${tb} where name = 'aa';" "count(1): 2"

	dmctl_stop_task $TASK_NAME $MASTER_PORT

	echo "start task in full mode and test not support"
	run_sql_file $cur/data/db1.prepare_err.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare_err.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	cat $cur/conf/dm-task.yaml >$WORK_DIR/dm-task.yaml
	TASK_NAME="full_err"
	sed -i "s/task-name-placeholder/${TASK_NAME}/g" $WORK_DIR/dm-task.yaml

	echo "start task and will failed"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task ${WORK_DIR}/dm-task.yaml" \
		"\"result\": false" 2 \
		"Error 1273: Unsupported collation when new collation is enabled: 'latin1_swedish_ci'" 1 \
		"Error 1273: Unsupported collation when new collation is enabled: 'utf8mb4_0900_ai_ci'" 1

	dmctl_stop_task $TASK_NAME $MASTER_PORT

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

	echo "start task in incremantal mode and test not support"

	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	TASK_NAME="incremental_err"
	cat $cur/conf/dm-task-increment.yaml >$WORK_DIR/dm-task.yaml
	sed -i "s/task-name-placeholder/${TASK_NAME}/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-name-placeholder-1/$name1/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-1/$pos1/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-1/$gtid1/g" $WORK_DIR/dm-task.yaml

	sed -i "s/binlog-name-placeholder-2/$name2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-pos-placeholder-2/$pos2/g" $WORK_DIR/dm-task.yaml
	sed -i "s/binlog-gtid-placeholder-2/$gtid2/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml

	run_sql_file $cur/data/db1.increment_err.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment_err.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "task sync and will failed"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status ${TASK_NAME}" \
		"Error 1273: Unsupported collation when new collation is enabled: 'latin1_swedish_ci'" 1 \
		"Error 1273: Unsupported collation when new collation is enabled: 'utf8mb4_0900_ai_ci'" 1

	dmctl_stop_task $TASK_NAME $MASTER_PORT
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
