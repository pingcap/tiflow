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
	wait_mysql_online.sh --port 4000

	# prepare data

	run_sql "SET @@GLOBAL.SQL_MODE='NO_BACKSLASH_ESCAPES'"
	run_sql "SET @@global.time_zone = '+01:00';"
	run_sql --port 4000 "SET @@global.time_zone = '+02:00';"
	run_sql --port 4000 "CREATE USER 'dm_full'@'%' IDENTIFIED BY '123456';"
	run_sql --port 4000 "GRANT ALL PRIVILEGES ON *.* TO 'dm_full'@'%';"
	run_sql --port 4000 "REVOKE ALTER ON *.* FROM 'dm_full'@'%';"

	run_sql_file $CUR_DIR/data/db1.prepare.sql

	# TODO we add retry on CreateWorker failed, but now IsRetryableError of DM always
	# return true, so we skip the following test cases. since engine is only used on
	# cloud, we have validate the config already before submit it, so it's ok now.
	# enable it later.
	#
	#	# test a ill-formatted job should fail
	#
	#	cp $CUR_DIR/conf/job.yaml $WORK_DIR/job.yaml
	#	sed -i "20,23d" $WORK_DIR/job.yaml
	#	job_id=$(create_job "DM" "$WORK_DIR/job.yaml" "dm_full_mode")
	#
	#	exec_with_retry "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | grep -q 'route-rules global not exist in routes'"
	#
	#	curl -X POST "http://127.0.0.1:10245/api/v1/jobs/$job_id/cancel"
	#	curl -X DELETE "http://127.0.0.1:10245/api/v1/jobs/$job_id"
	#	curl "http://127.0.0.1:10245/api/v1/jobs/$job_id" | grep -q "ErrJobNotFound"
	#
	#	# test stop a wrongly configuration job
	#
	#	cp $CUR_DIR/conf/job.yaml $WORK_DIR/job.yaml
	#	sed -i "s/dm_full/wrong_user/g" $WORK_DIR/job.yaml
	#
	#	job_id=$(create_job "DM" "$WORK_DIR/job.yaml" "dm_full_mode1")
	#
	#	exec_with_retry "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | grep -q 'Access denied'"
	#
	#	curl -X POST "http://127.0.0.1:10245/api/v1/jobs/$job_id/cancel"
	#	curl -X DELETE "http://127.0.0.1:10245/api/v1/jobs/$job_id"

	# test downstream has no ALTER privilege
	run_sql "create table dm_full.auto(c int primary key auto_increment);"
	run_sql "insert into dm_full.auto values(1),(2);"

	# create job & wait for job finished
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "12312" "dm_full_mode")
	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | grep -q 'Error 1142 (42000): ALTER command denied'"
	docker restart server-executor-0 server-executor-1 server-executor-2
	# check the error is not related to lightning checkpoint
	exec_with_retry --count 60 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | grep -q 'Error 1142 (42000): ALTER command denied'"

	run_sql --port 4000 "GRANT ALTER ON *.* TO 'dm_full'@'%';"
	curl -X PUT "http://127.0.0.1:10245/api/v1/jobs/$job_id/status" -H 'Content-Type: application/json' -d '{"op": "resume"}'

	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"
	# curl http://127.0.0.1:10245/api/v1/jobs/$job_id | tee /dev/stderr | jq -r '.detail' | base64 --decode | jq -e '.finished_unit_status."mysql-01"[1].Status.finishedBytes == 614'

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
