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
	run_sql "SET @@GLOBAL.SQL_MODE='PIPES_AS_CONCAT,IGNORE_SPACE,ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,NO_AUTO_VALUE_ON_ZERO,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,REAL_AS_FLOAT'"
	run_sql --port 3307 "SET @@GLOBAL.SQL_MODE=''"
	run_sql_file $CUR_DIR/data/db1.prepare.sql
	run_sql_file --port 3307 $CUR_DIR/data/db2.prepare.sql

	# create job & wait for job finished
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "dm_sql_mode")
	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-02\".status.unit == \"DMSyncTask\"'"

	# check data
	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml

	# insert increment data
	run_sql_file $CUR_DIR/data/db1.increment.sql
	run_sql_file --port 3307 $CUR_DIR/data/db2.increment.sql
	run_sql_file $CUR_DIR/data/timezone.Asia-Shanghai.sql
	run_sql_file $CUR_DIR/data/timezone.America-Phoenix.sql

	# check data
	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml

	# check resource is cleaned
	mc alias set myminio http://127.0.0.1:9000 engine engineSecret
	mc find myminio --name metadata | wc -l | grep -q 0
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
