#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
db1="downstream_more_column1"
tb1="t1"
db="downstream_more_column"
tb="t"

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# create table in tidb with AUTO_INCREMENT
	run_sql_file $cur/data/tidb.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# start DM task in all mode
	# schemaTracker create table from dump data
	dmctl_start_task_standalone "$cur/conf/dm-task.yaml" "--remove-meta"
	# check full load data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1<100;" "count(1): 2"

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"sourceTable: \`${db1}\`.\`${tb1}\`" 1 \
		"targetTable: \`${db}\`.\`${tb}\`" 1 \
		"Column count doesn't match value count" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-01 test -d ${db1} -t ${tb1} $cur/data/schema.sql" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"
	# check incremental data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1>100 and c1<1000;" "count(1): 2"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2

	# start DM task in incremental mode
	# schemaTracker create table from downstream
	master_status=($(get_master_status))
	cp $cur/conf/dm-task-incremental.yaml $WORK_DIR/dm-task-incremental.yaml
	sed -i "s/binlog-gtid-placeholder/${master_status[2]}/g" $WORK_DIR/dm-task-incremental.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task-incremental.yaml --remove-meta" \
		"\"result\": true" 2

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# Column count doesn't match value count
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"sourceTable: $(${db1}).$(${tb1})" 1 \
		"targetTable: $(${db}).$(${tb})" 1 \
		"Column count doesn't match value count" 1

	# operate-schema: flush checkpoint default
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-01 test -d ${db1} -t ${tb1} $cur/data/schema.sql" \
		"\"result\": true" 2
	check_log_contain_with_retry 'flush table info' $WORK_DIR/worker1/log/dm-worker.log

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	# check incremental data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1>1000 and c1<10000;" "count(1): 2"
}

cleanup_data downstream_more_column
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
