#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e_with_s3.yaml $DOCKER_COMPOSE_DIR/dm_databases.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"

function run() {
	start_engine_cluster $CONFIG
	wait_mysql_online.sh --port 3306
	wait_mysql_online.sh --port 3307
	wait_mysql_online.sh --port 4000

	# prepare MySQL global variables and data
	run_sql_file --port 3306 $CUR_DIR/data/db1.prepare.sql
	run_sql_file --port 3307 $CUR_DIR/data/db2.prepare.sql

	# create job & wait for job finished
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "checkpoint")
	job_id2=$(create_job "DM" "$CUR_DIR/conf/job2.yaml" "checkpoint2")
	exec_with_retry --count 100 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"
	exec_with_retry --count 100 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id2\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"
	# clean the lighting checkpoint in downstream after job is finished
	exec_with_retry --count 30 '! run_sql --port 4000 "show databases;" | grep -q "checkpoint"'
	exec_with_retry --count 30 '! run_sql --port 4000 "show databases;" | grep -q "checkpoint2"'

	# check data
	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml

	# check resource is cleaned
	mc alias set myminio http://127.0.0.1:9000 engine engineSecret
	mc find myminio --name metadata | wc -l | grep -q 0
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
