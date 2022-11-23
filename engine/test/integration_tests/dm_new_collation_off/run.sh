#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e_with_s3.yaml $DOCKER_COMPOSE_DIR/dm_databases_tidb_new_collation_off.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"

function run() {
	start_engine_cluster $CONFIG
	wait_mysql_online.sh --port 3307
	wait_mysql_online.sh --port 4000

	# prepare data
	run_sql_file --port 3307 $CUR_DIR/data/db2.prepare.sql

	# create job & wait for job finished
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "dm_new_collation_off")
	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-02\".status.unit == \"DMSyncTask\"'"

	# check data
	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml

	# insert increment data
	run_sql_file --port 3307 $CUR_DIR/data/db2.increment.sql

	# check data
	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
