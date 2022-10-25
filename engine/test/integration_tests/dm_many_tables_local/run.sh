#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml $DOCKER_COMPOSE_DIR/dm_databases.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"
TABLE_NUM=500

function run() {
	start_engine_cluster $CONFIG
	wait_mysql_online.sh --port 3306
	wait_mysql_online.sh --port 4000

	# prepare data
	run_sql 'DROP DATABASE IF EXISTS dm_many_tables'
	run_sql 'CREATE DATABASE dm_many_tables;'
	for i in $(seq $TABLE_NUM); do
		run_sql --quiet "CREATE TABLE dm_many_tables.t$i(i TINYINT, j INT UNIQUE KEY);"
		for j in $(seq 2); do
			run_sql --quiet "INSERT INTO dm_many_tables.t$i VALUES ($j,${j}000$j),($j,${j}001$j);"
		done
		# to make the tables have odd number of lines before 'ALTER TABLE' command, for check_sync_diff to work correctly
		run_sql --quiet "INSERT INTO dm_many_tables.t$i VALUES (9, 90009);"
	done

	# create job & wait for job finished
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "dm_many_tables")
	# check progress is forwarded gradually, not jump to "finished"
	exec_with_retry --count 500 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status.status | .finishedBytes > 0 and .finishedBytes < .totalBytes'"
	exec_with_retry --count 100 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"

	# check data
	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml 1
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
