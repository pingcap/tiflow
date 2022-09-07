#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml $DOCKER_COMPOSE_DIR/dm_databases.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"

function run() {
	start_engine_cluster $CONFIG
	wait_mysql_online.sh --port 3306 --try-nums 100
	wait_mysql_online.sh --port 4000 --try-nums 100

	# prepare data

	run_sql "SET @@GLOBAL.SQL_MODE='NO_BACKSLASH_ESCAPES'"
	run_sql "SET @@global.time_zone = '+01:00';"
	run_sql --port 4000 "SET @@global.time_zone = '+02:00';"

	run_sql_file $CUR_DIR/data/db1.prepare.sql

	# create job

	create_job_json=$(base64 -w0 $CUR_DIR/conf/job.yaml | jq -Rs '{ type: "DM", config: . }')
	echo "create_job_json: $create_job_json"
	job_id=$(curl -X POST -H "Content-Type: application/json" -d "$create_job_json" "http://127.0.0.1:10245/api/v1/jobs?tenant_id=dm_full_mode&project_id=dm_full_mode" | jq -r .id)
	echo "job_id: $job_id"

	# wait for job finished
	exec_with_retry "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"

	# check data

	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml 1

	run_sql --port 4000 "SHOW DATABASES LIKE 'dm_full_route_schema_succ'\G" | grep -q ": dm_full_route_schema_succ"

	run_sql "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
	run_sql "SET @@GLOBAL.TIME_ZONE='SYSTEM';"
	run_sql --port 4000 "SET @@GLOBAL.TIME_ZONE='SYSTEM';"
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
