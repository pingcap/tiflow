#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e_with_s3.yaml $DOCKER_COMPOSE_DIR/dm_databases.yaml"
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

	# create job & wait for job to enter load phase
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "dm_many_tables")
	exec_with_retry --count 500 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status.status | .finishedBytes > 0 and .finishedBytes < .totalBytes'"

	# test autoresume
	docker stop dm_downstream_tidb
	exec_with_retry --count 20 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status | .unit == \"DMLoadTask\" and .stage == \"Error\"'"
	docker start dm_downstream_tidb
	docker restart server-executor-0 server-executor-1 server-executor-2

	# wait jobMaster online
	exec_with_retry --count 50 --interval_sec 10 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | grep 'job_id'"
	exec_with_retry --count 500 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status.status | .finishedBytes > 0 and .finishedBytes < .totalBytes'"

	# test pause and resume
	exec_with_retry --count 20 "curl -X PUT \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" -H 'Content-Type: application/json' -d '{\"op\": \"pause\"}'"
	sleep 10
	exec_with_retry --count 20 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status | .stage == \"Paused\"'"
	exec_with_retry --count 20 "curl -X PUT \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" -H 'Content-Type: application/json' -d '{\"op\": \"resume\"}'"
	exec_with_retry --count 20 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status | .stage == \"Running\"'"

	# wait for job finished and check data
	exec_with_retry --count 50 --interval_sec 10 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"
	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml 1
}

function stop {
	if [ ! -z $job_id ]; then
		echo -e "\n\nquery job statu before stop dm_many_tables...\n"
		curl "http://127.0.0.1:10245/api/v1/jobs/$job_id/status" || true
		curl "http://127.0.0.1:10245/api/v1/jobs/$job_id" || true
	fi
	stop_engine_cluster $WORK_DIR $CONFIG
}

trap stop EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
