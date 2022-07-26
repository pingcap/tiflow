#!/bin/bash

set -eux

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="s3_dumpling_lightning"
SPECIAL_TASK_NAME="ab?c/b%cËd"
SOURCE_ID1="mysql-replica-01"
db="s3_dumpling_lightning"
db1="s3_dumpling_lightning1"
tb="t"
tb1="t1"
S3_DIR="s3://dmbucket/dump?region=us-west-2\&endpoint=http://127.0.0.1:8688\&access_key=s3accesskey\&secret_access_key=s3secretkey\&force_path_style=true"
LOCAL_TEST_DIR="./dumpdata"

# s3 config
s3_ACCESS_KEY="s3accesskey"
s3_SECRET_KEY="s3secretkey"
S3_ENDPOINT="127.0.0.1:8688"
s3_DBPATH="${WORK_DIR}/s3.minio"
s3_bucket="dmbucket"
dumpPath="dmbucket/dump"

# start s3 server
function start_s3() {
	export MINIO_ACCESS_KEY=$s3_ACCESS_KEY
	export MINIO_SECRET_KEY=$s3_SECRET_KEY
	export MINIO_BROWSER=on
	export S3_ENDPOINT=$S3_ENDPOINT
	bin/minio server --address $S3_ENDPOINT "$s3_DBPATH" &
	s3_MINIO_PID=$!

	i=0
	while ! curl -o /dev/null -v -s "http://$S3_ENDPOINT/"; do
		i=$(($i + 1))
		if [ $i -gt 7 ]; then
			echo 'Failed to start minio'
			exit 1
		fi
		sleep 2
	done
	# create bucket dbpath
	mkdir -p "${s3_DBPATH}/${s3_bucket}"
}

# clean s3 server
cleanup_s3() {
	pkill -9 minio 2>/dev/null || true
	wait_process_exit minio
	rm -rf $s3_DBPATH
}

# check dump file in s3
# $1 db name
# $2 table name
# $3 task name
# $4 source id
function check_dump_s3_exist() {

	schema_create="${1}-schema-create.sql"
	table_schema="${1}.${2}-schema.sql"

	file_should_exist "${s3_DBPATH}/${dumpPath}/${3}.${4}/${schema_create}"
	file_should_exist "${s3_DBPATH}/${dumpPath}/${3}.${4}/${table_schema}"
}

function file_should_exist() {
	if [ ! -f "$1" ]; then
		echo "[$(date)] File $1 not found." && exit 1
	fi
}

function dir_should_not_exist() {
	if [ -d "$1" ]; then
		echo "[$(date)] Dir $1 should not found." && exit 1
	fi
}

# $1 == true will checkDumpFile, false will not
# $2 == task_name used for check dump file exist or not
function run_test() {

	cleanup_data
	cleanup_s3
	# start s3 server
	start_s3

	kill_dm_master
	kill_dm_worker

	# start dm master and worker
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

	echo "prepare source data"
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "start task"
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s#name: test#name: $2#g" $WORK_DIR/dm-task.yaml
	sed -i "s#dir: placeholder#dir: $S3_DIR#g" $WORK_DIR/dm-task.yaml
	if $1; then
		sed -i "s#clean-dump-file: true#clean-dump-file: false#g" $WORK_DIR/dm-task.yaml
	fi
	dmctl_start_task $WORK_DIR/dm-task.yaml "--remove-meta"

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check task result"
	# wait
	run_sql_tidb_with_retry "select count(1) from information_schema.tables where TABLE_SCHEMA='${db}' and TABLE_NAME = '${tb}';" "count(1): 1"

	# check table data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 25"

	# check dump file
	if $1; then
		check_dump_s3_exist $db1 $tb1 $2 $SOURCE_ID1
	else
		dir_should_not_exist "${s3_DBPATH}/${dumpPath}/${2}.${SOURCE_ID1}"
	fi

	cleanup_s3
}

function run_error_check() {

	cleanup_data
	cleanup_s3
	# start s3 server
	start_s3

	kill_dm_master
	kill_dm_worker

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/TestRemoveMetaFile=return()"

	# start dm master and worker
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

	echo "prepare source data"
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "start task"
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s#name: test#name: $TASK_NAME#g" $WORK_DIR/dm-task.yaml
	sed -i "s#dir: placeholder#dir: $S3_DIR#g" $WORK_DIR/dm-task.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml"

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "error check"
	check_log_contain_with_retry 'panic: success check file not exist!!' $WORK_DIR/worker1/log/stdout.log
	check_log_contain_with_retry 'panic: success check file not exist!!' $WORK_DIR/worker2/log/stdout.log

	export GO_FAILPOINTS=""

	cleanup_s3
}

function test_local_special_name() {
	cleanup_data

	kill_dm_master
	kill_dm_worker

	# start dm master and worker
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

	echo "prepare source data"
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "start task"
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s#name: test#name: $SPECIAL_TASK_NAME#g" $WORK_DIR/dm-task.yaml
	sed -i "s#dir: placeholder#dir: $LOCAL_TEST_DIR#g" $WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml "--remove-meta"

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check task result"
	# wait
	run_sql_tidb_with_retry "select count(1) from information_schema.tables where TABLE_SCHEMA='${db}' and TABLE_NAME = '${tb}';" "count(1): 1"

	# check table data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 25"
}

function run() {
	run_test true $TASK_NAME
	echo "run s3 test with check dump files success"
	run_test false $TASK_NAME
	echo "run s3 test without check dump files success"
	run_test true $SPECIAL_TASK_NAME
	echo "run s3 test with special task-name and check dump files success"
	run_test false $SPECIAL_TASK_NAME
	echo "run s3 test with special task-name and without check dump files success"
	run_error_check
	echo "run s3 test error check success"
	# # TODO local special name will be resolved after fix https://github.com/pingcap/tidb/issues/32549
	# test_local_special_name
	# echo "run local special task-name success"
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
