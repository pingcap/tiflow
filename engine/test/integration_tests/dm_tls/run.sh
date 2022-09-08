#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml $DOCKER_COMPOSE_DIR/dm_databases_with_tls.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"

function run() {
  generate_cert /tmp/certs/downstream tidb
	start_engine_cluster $CONFIG

	# copy auto-generated certificates from MySQL to bypass permission
	mkdir -p $WORK_DIR/mysql1
	mkdir -p $WORK_DIR/mysql2
	sudo cat /tmp/mysql1/client-key.pem > $WORK_DIR/mysql1/client-key.pem
	sudo cat /tmp/mysql1/client-cert.pem > $WORK_DIR/mysql1/client-cert.pem
	sudo cat /tmp/mysql2/client-key.pem > $WORK_DIR/mysql2/client-key.pem
	sudo cat /tmp/mysql2/client-cert.pem > $WORK_DIR/mysql2/client-cert.pem

	wait_mysql_online.sh --password 123456 --ssl-key $WORK_DIR/mysql1/client-key.pem --ssl-cert $WORK_DIR/mysql1/client-cert.pem
	wait_mysql_online.sh --port 3307 --password 123456 --ssl-key $WORK_DIR/mysql2/client-key.pem --ssl-cert $WORK_DIR/mysql2/client-cert.pem

  echo "verify can't connect to upstream without certificates"
  mysql -P3306 -h127.0.0.1 -uroot -p123456 -e "show databases" && echo "failed" && exit 1 || true

	# prepare data

  run_sql_file --password 123456 --ssl-key $WORK_DIR/mysql1/client-key.pem --ssl-cert $WORK_DIR/mysql1/client-cert.pem $CUR_DIR/data/db1.prepare.sql
	run_sql_file --port 3307 --password 123456 --ssl-key $WORK_DIR/mysql2/client-key.pem --ssl-cert $WORK_DIR/mysql2/client-cert.pem $CUR_DIR/data/db2.prepare.sql

	# create downstream user

	run_sql --port 4000 --ssl-key /tmp/certs/downstream/client.key --ssl-cert /tmp/certs/downstream/client.pem "CREATE USER 'dm_user'@'%' REQUIRE X509;"

	# create job

  cp $CUR_DIR/conf/job.yaml $WORK_DIR/job.yaml
  sed -i "s,<downstream-key>,$(base64 -w0 /tmp/certs/downstream/client.key)," $WORK_DIR/job.yaml
  sed -i "s,<downstream-cert>,$(base64 -w0 /tmp/certs/downstream/client.pem)," $WORK_DIR/job.yaml
  sed -i "s,<mysql1-key>,$(base64 -w0 $WORK_DIR/mysql1/client-key.pem)," $WORK_DIR/job.yaml
  sed -i "s,<mysql1-cert>,$(base64 -w0 $WORK_DIR/mysql1/client-cert.pem)," $WORK_DIR/job.yaml
  sed -i "s,<mysql2-key>,$(base64 -w0 $WORK_DIR/mysql2/client-key.pem)," $WORK_DIR/job.yaml
  sed -i "s,<mysql2-cert>,$(base64 -w0 $WORK_DIR/mysql2/client-cert.pem)," $WORK_DIR/job.yaml

	create_job_json=$(base64 -w0 $WORK_DIR/job.yaml | jq -Rs '{ type: "DM", config: . }')
	echo "create_job_json: $create_job_json"
	job_id=$(curl -X POST -H "Content-Type: application/json" -d "$create_job_json" "http://127.0.0.1:10245/api/v1/jobs?tenant_id=dm_tls&project_id=dm_tls" | jq -r .id)
	echo "job_id: $job_id"

	# wait for dump and load finished

read -p 123
	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-02\".status.unit == 12'"

	# check data

	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml

	# insert increment data

	run_sql_file --port 3307 $CUR_DIR/data/db2.increment.sql

	# check data

	check_sync_diff $WORK_DIR $CUR_DIR/conf/diff_config.toml
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
# TODO: handle log properly
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
