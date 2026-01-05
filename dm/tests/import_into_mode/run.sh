#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"
SOURCE_ID1="mysql-replica-01"
db="import_into_mode"

# s3 config
s3_ACCESS_KEY="s3accesskey"
s3_SECRET_KEY="s3secretkey"
S3_ENDPOINT="127.0.0.1:8688"
s3_DBPATH="${WORK_DIR}/s3.minio"
s3_bucket="dmbucket"
dumpPath="dmbucket/dump"
S3_DIR="s3://dmbucket/dump?region=us-east-1\&endpoint=http://127.0.0.1:8688\&access_key=s3accesskey\&secret_access_key=s3secretkey\&force_path_style=true"

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

function run_basic_test() {
	echo "=== Running import-into mode basic test ==="

	run_sql_tidb "drop database if exists ${db};"

	cleanup_s3
	start_s3

	echo "prepare source data"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	echo "start task with import-into mode"
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s#dir: placeholder#dir: $S3_DIR#g" $WORK_DIR/dm-task.yaml
	dmctl_start_task_standalone $WORK_DIR/dm-task.yaml "--remove-meta"

	# wait for load to complete and sync to start
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"unit": "Sync"' 1

	echo "check full dump data"
	run_sql_tidb_with_retry "select count(1) from ${db}.t1;" "count(1): 4"
	run_sql_tidb_with_retry "select count(1) from ${db}.t2;" "count(1): 3"

	echo "insert incremental data"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# use sync_diff_inspector to check data
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "verify final data counts"
	run_sql_tidb_with_retry "select count(1) from ${db}.t1;" "count(1): 6"
	run_sql_tidb_with_retry "select count(1) from ${db}.t2;" "count(1): 5"

	echo "check dump files have been cleaned"
	ls $WORK_DIR/worker1/dumped_data.test && exit 1 || echo "worker1 auto removed dump files"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2

	echo "=== import-into mode basic test passed ==="
}

function run_sharding_reject_test() {
	echo "=== Running import-into mode sharding rejection test ==="

	run_sql_tidb "drop database if exists ${db};"

	cleanup_s3
	start_s3

	echo "try to start task with import-into mode + sharding (should fail)"
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task-sharding.yaml
	sed -i "s#dir: placeholder#dir: $S3_DIR#g" $WORK_DIR/dm-task-sharding.yaml
	sed -i "s#is-sharding: false#is-sharding: true#g" $WORK_DIR/dm-task-sharding.yaml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task-sharding.yaml --remove-meta" \
		"import-into mode does not support sharding" 1

	echo "=== import-into mode sharding rejection test passed ==="
}

function run_local_dir_reject_test() {
	echo "=== Running import-into mode local dir rejection test ==="

	run_sql_tidb "drop database if exists ${db};"
	
	echo "try to start task with import-into mode + local dir (should fail)"
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task-local.yaml
	sed -i "s#dir: placeholder#dir: ./local_data#g" $WORK_DIR/dm-task-local.yaml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task-local.yaml --remove-meta" \
		"import-into mode requires shared storage" 1

	echo "=== import-into mode local dir rejection test passed ==="
}

function run_ha_failover_test() {
	echo "=== Running import-into mode HA failover test ==="

	run_sql_tidb "drop database if exists ${db};"

	cleanup_s3
	start_s3

	# start worker2 (available for failover)
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "prepare source data"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	echo "start task with import-into mode"
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s#dir: placeholder#dir: $S3_DIR#g" $WORK_DIR/dm-task.yaml
	dmctl_start_task_standalone $WORK_DIR/dm-task.yaml "--remove-meta"

	# wait until the task enters the Load phase, then kill worker1 to simulate failover
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"unit": "Load"' 1
	echo "killing worker1 to simulate failover (after Load started)"
	kill_process $WORK_DIR/worker1 || true

	# wait for sync to resume on remaining worker
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"unit": "Sync"' 1

	echo "check full dump data after failover"
	run_sql_tidb_with_retry "select count(1) from ${db}.t1;" "count(1): 4"
	run_sql_tidb_with_retry "select count(1) from ${db}.t2;" "count(1): 3"

	echo "insert incremental data"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# use sync_diff_inspector to check data
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "verify final data counts"
	run_sql_tidb_with_retry "select count(1) from ${db}.t1;" "count(1): 6"
	run_sql_tidb_with_retry "select count(1) from ${db}.t2;" "count(1): 5"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2

	echo "=== import-into mode HA failover test passed ==="
}

function run() {
	# start full TiDB cluster (PD + TiKV + TiDB) for import-into mode
	run_downstream_cluster $WORK_DIR

	# start dm master and worker
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_basic_test
	run_local_dir_reject_test
	run_sharding_reject_test
	run_ha_failover_test
}

# cleanup work dir
rm -rf $WORK_DIR
mkdir -p $WORK_DIR

# also cleanup dm processes in case of last run failed
cleanup_process $*
killall tidb-server 2>/dev/null || true
killall tikv-server 2>/dev/null || true
killall pd-server 2>/dev/null || true
run $*
cleanup_process $*
killall pd-server 2>/dev/null || true
killall tikv-server 2>/dev/null || true
killall tidb-server 2>/dev/null || true

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
