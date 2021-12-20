#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="gbk"
db="gbk"
db_increment="gbk_increment"
db_server_collation="gbk_server"
db2="gbk2"
db_increment2="gbk_increment2"
db_server_collation2="gbk_server2"
tb="t1"
tb2="t2"
tb_check="t_check"

function run() {
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

	echo "prepare data"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "start task"
	dmctl_start_task $cur/conf/dm-task.yaml "--remove-meta"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "prepare incremental data"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check incremental phase"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data gbk gbk2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
