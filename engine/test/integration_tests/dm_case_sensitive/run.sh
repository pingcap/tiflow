#!/bin/bash

set -eu

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK_DIR=$OUT_DIR/$TEST_NAME

CONFIG="$DOCKER_COMPOSE_DIR/3m3e_with_s3.yaml $DOCKER_COMPOSE_DIR/dm_databases.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"
TABLE_NUM=500

function run() {
	start_engine_cluster $CONFIG
	wait_mysql_online.sh --port 3306
	wait_mysql_online.sh --port 3307
	wait_mysql_online.sh --port 4000

	# prepare data

	run_sql_file $CUR_DIR/data/db1.prepare.sql
	run_sql_file --port 3307 $CUR_DIR/data/db2.prepare.sql
	# manually create the route table
	run_sql --port 4000 'CREATE DATABASE IF NOT EXISTS \`UPPER_DB_ROUTE\`'

	# create job
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "dm_case_sensitive")
	# wait for job finished
	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"

	# check data

	exec_with_retry 'run_sql --port 4000 "show databases;" | grep -q "Upper_DB1"'
	exec_with_retry 'run_sql --port 4000 "show databases;" | grep -q "lower_db"'
	exec_with_retry 'run_sql --port 4000 "show databases;" | grep -q "UPPER_DB_ROUTE"'
	exec_with_retry '! run_sql --port 4000 "show databases;" | grep -q "Upper_Db_IGNORE"'
	exec_with_retry '! run_sql --port 4000 "show tables from UPPER_DB_ROUTE;" | grep -q "Do_table_ignore"'
	exec_with_retry 'run_sql --port 4000 "select count(*) from UPPER_DB_ROUTE.do_table_route\G" | grep -Fq "count(*): 2"'
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
