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
	wait_mysql_online.sh --port 3307
	wait_mysql_online.sh --port 4000

	# prepare data

	run_sql_file $CUR_DIR/data/db1.prepare.sql
	run_sql_file --port 3307 $CUR_DIR/data/db2.prepare.sql
	# manually create the route table
  run_sql --port 4000 'CREATE DATABASE IF NOT EXISTS `UPPER_DB_ROUTE`'

	# create job

	create_job_json=$(jq -Rs '{ job_type: 3, tenant_id: "dm_full_mode", project_id: "dm_full_mode", job_config: . }' $CUR_DIR/conf/job.yaml)
	echo "create_job_json: $create_job_json"
	job_id=$(curl -X POST -H "Content-Type: application/json" -d "$create_job_json" http://127.0.0.1:10245/api/v1/jobs)
	echo "job_id: $job_id"

	# wait for job finished

	# remove quotes
	job_id=${job_id:1:-1}
	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.TaskStatus.\"mysql-01\".Status.Stage == 4 and .TaskStatus.\"mysql-02\".Status.Stage == 4'"

	# check data

	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml 1

	# test block-allow-list by the way
	run_sql "show databases;" $TIDB_PORT $TIDB_PASSWORD
	check_contains "Upper_DB1"
	check_contains "lower_db"
	# test route-rule
	check_contains "UPPER_DB_ROUTE"

	run_sql "show tables from UPPER_DB_ROUTE" $TIDB_PORT $TIDB_PASSWORD
	check_contains "do_table_route"
	run_sql_tidb_with_retry "select count(*) from UPPER_DB_ROUTE.do_table_route" "count(*): 5"
}

trap "stop_engine_cluster $CONFIG" EXIT
run $*
# TODO: handle log properly
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
