#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml $DOCKER_COMPOSE_DIR/dm_databases_tidb_new_collation_off.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"

function run() {
	start_engine_cluster $CONFIG
	wait_mysql_online.sh --port 3307 --try-nums 100
	wait_mysql_online.sh --port 4000 --try-nums 100

	# prepare data

	run_sql_file --port 3307 $CUR_DIR/data/db2.prepare.sql

	# create job

	create_job_json=$(base64 -w0 $CUR_DIR/conf/job.yaml | jq -Rs '{ type: "DM", config: . }')
	echo "create_job_json: $create_job_json"
	job_id=$(curl -X POST -H "Content-Type: application/json" -d "$create_job_json" "http://127.0.0.1:10245/api/v1/jobs?tenant_id=dm_case_sensitive&project_id=dm_case_sensitive" | jq -r .id)
	echo "job_id: $job_id"

	# wait for dump and load finished

	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-02\".status.unit == 12'"

	# check data

	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml

	# insert increment data

	run_sql_file --port 3307 $CUR_DIR/data/db2.increment.sql

	# check data

	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
# TODO: handle log properly
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
