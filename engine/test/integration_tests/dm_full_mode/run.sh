#!/bin/bash

set -eu

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK_DIR=$OUT_DIR/$TEST_NAME
CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml $DOCKER_COMPOSE_DIR/dm_databases.yaml"
TABLE_NUM=500

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_engine_cluster $CONFIG
	wait_mysql_online.sh --port 3306
	wait_mysql_online.sh --port 4000

	# prepare data

	run_sql 'DROP DATABASE IF EXISTS dm_full_mode'
	run_sql 'CREATE DATABASE dm_full_mode;'
	for i in $(seq $TABLE_NUM); do
		run_sql "CREATE TABLE dm_full_mode.t$i(i TINYINT, j INT UNIQUE KEY);"
		for j in $(seq 2); do
			run_sql "INSERT INTO dm_full_mode.t$i VALUES ($j,${j}000$j),($j,${j}001$j);"
		done
		# to make the tables have odd number of lines before 'ALTER TABLE' command, for check_sync_diff to work correctly
		run_sql "INSERT INTO dm_full_mode.t$i VALUES (9, 90009);"
	done

	# create job

	create_job_json=$(jq -Rs '{ job_type: 3, tenant_id: "dm_full_mode", project_id: "dm_full_mode", job_config: . }' $CUR_DIR/conf/job.yaml)
	echo "create_job_json: $create_job_json"
	job_id=$(curl -X POST -H "Content-Type: application/json" -d "$create_job_json" http://127.0.0.1:10245/api/v1/jobs)
	echo "job_id: $job_id"

	# wait for job finished

	# remove quotes
	job_id=${job_id:1:-1}
	exec_with_retry --count 200 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | jq -e '.TaskStatus.\"mysql-01\".Status.Stage == 4'"

	# check data

	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml 1
}

trap "stop_engine_cluster $CONFIG" EXIT
run $*
# TODO: handle log properly
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
