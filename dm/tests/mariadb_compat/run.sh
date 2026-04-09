#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

MARIADB_HOST=${MARIADB_HOST1:-127.0.0.1}
MARIADB_PORT=${MARIADB_PORT1:-3308}
MARIADB_PASSWORD=${MARIADB_PASSWORD1:-123456}

function prepare_source_cfg() {
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "s|host: 127.0.0.1|host: ${MARIADB_HOST}|" $WORK_DIR/source1.yaml
	sed -i "s|port: 3308|port: ${MARIADB_PORT}|" $WORK_DIR/source1.yaml
	sed -i "s|password: '123456'|password: '${MARIADB_PASSWORD}'|" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
}

function check_transformed_schema() {
	run_sql_tidb_with_retry "show create table mariadbcompat.t_full" "utf8mb4_0900_ai_ci"

	run_sql_tidb "show create table mariadbcompat.t_full"
	check_contains "varchar(768)"
	check_contains "KEY \`idx_txt\` (\`txt\`(255))"
	check_contains "KEY \`idx_v\` (\`v\`"
	check_not_contains "DEFAULT 'x'"
	check_not_contains "json_valid"
}

function check_loaded_data() {
	run_sql_tidb_with_retry "select count(*) from mariadbcompat.t_full" "count(*): 2"
	run_sql_tidb "select txt from mariadbcompat.t_full where id = 1"
	check_contains "alpha"
	run_sql_tidb "select json_extract(j, '$.a') as a from mariadbcompat.t_full where id = 2"
	check_contains "2"
}

function check_incremental_schema() {
	run_sql_tidb_with_retry "show create table mariadbcompat.t_replace" "utf8mb4_0900_ai_ci"

	run_sql_tidb "show create table mariadbcompat.t_replace"
	check_contains "KEY \`idx_note\` (\`note\`(255))"
	check_not_contains "DEFAULT 'x'"
	check_not_contains "DEFAULT 'y'"
	check_contains "\`late_txt\` text"

	run_sql_tidb_with_retry "select count(*) from mariadbcompat.t_replace" "count(*): 2"
	run_sql_tidb "select note from mariadbcompat.t_replace where id = 2"
	check_contains "second"
}

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MARIADB_HOST $MARIADB_PORT $MARIADB_PASSWORD

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	prepare_source_cfg
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_start_task_standalone "$cur/conf/dm-task.yaml" "--remove-meta"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"unit\": \"Sync\"" 1 \
		"\"stage\": \"Running\"" 2

	check_transformed_schema
	check_loaded_data

	run_sql_file $cur/data/db1.increment.sql $MARIADB_HOST $MARIADB_PORT $MARIADB_PASSWORD
	check_incremental_schema
}

cleanup_data mariadbcompat
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
