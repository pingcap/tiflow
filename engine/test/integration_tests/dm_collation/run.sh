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
	wait_mysql_online.sh --port 3307
	wait_mysql_online.sh --port 4000

	# change default charset and collation for MySQL 8.0
	run_sql --port 3307 "set global character_set_server='utf8mb4';set global collation_server='utf8mb4_bin';"

	# prepare data

	run_sql_file $CUR_DIR/data/db1.prepare.sql
	run_sql_file --port 3307 $CUR_DIR/data/db2.prepare.sql

	# create job

	create_job_json=$(base64 -w0 $CUR_DIR/conf/job.yaml | jq -Rs '{ type: "DM", config: . }')
	echo "create_job_json: $create_job_json"
	job_id=$(curl -X POST -H "Content-Type: application/json" -d "$create_job_json" "http://127.0.0.1:10245/api/v1/jobs?tenant_id=dm_case_sensitive&project_id=dm_case_sensitive" | jq -r .id)
	echo "job_id: $job_id"

	# TODO: blocked by https://github.com/pingcap/tiflow/issues/6856
	#	# utf8mb4_0900_as_cs is not supported, should display error message
	#
	#	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -r '.task_status.\"mysql-02\".status.result.errors[0].message' | grep -q \"Error 1273: Unknown collation: 'utf8mb4_0900_as_cs'\""
	#	curl -X POST "http://127.0.0.1:10245/api/v1/jobs/$job_id/cancel"
	#	curl -X DELETE "http://127.0.0.1:10245/api/v1/jobs/$job_id"
	#	# change allow-list
	#	# clean downstraem data

	# wait for dump and load finished

	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status.unit == \"DMSyncTask\" and .task_status.\"mysql-02\".status.unit == \"DMSyncTask\"'"

	# check data

	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation.t1 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation.t2 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation2.t1 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation2.t2 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'

	# insert increment data

	run_sql_file $CUR_DIR/data/db1.increment.sql
	run_sql_file --port 3307 $CUR_DIR/data/db2.increment.sql

	# check data

	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation_increment.t1 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation_increment.t2 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation_increment2.t1 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation_increment2.t2 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation_server.t1 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
	exec_with_retry 'run_sql --port 4000 "select count(1) from sync_collation_server2.t1 where name =' "'aa'" '\G" | grep -Fq "count(1): 2"'
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
# run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
