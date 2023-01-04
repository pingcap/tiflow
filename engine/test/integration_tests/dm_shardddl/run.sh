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
	wait_mysql_online.sh --port 3307
	wait_mysql_online.sh --port 4000

	# prepare full data
	run_sql --port 3306 "drop database if exists dm_shardddl;"
	run_sql --port 3307 "drop database if exists dm_shardddl;"
	run_sql --port 3306 "create database dm_shardddl character set utf8mb4 collate utf8mb4_bin;"
	run_sql --port 3307 "create database dm_shardddl character set utf8mb4 collate utf8mb4_bin;"
	run_sql --port 3306 "create table dm_shardddl.t1 (id int, name varchar(20), primary key(id)) character set utf8mb4 collate utf8mb4_bin;"
	run_sql --port 3306 "create table dm_shardddl.t2 (id int, name varchar(20), primary key(id)) character set utf8mb4 collate utf8mb4_bin;"
	run_sql --port 3307 "create table dm_shardddl.t1 (id int, name varchar(20), primary key(id)) character set utf8mb4 collate utf8mb4_bin;"
	run_sql --port 3307 "create table dm_shardddl.t2 (id int, name varchar(20), primary key(id)) character set utf8mb4 collate utf8mb4_bin;"
	run_sql --port 3306 "insert into dm_shardddl.t1 values(1,'aaa');"
	run_sql --port 3306 "insert into dm_shardddl.t2 values(2,'bbb');"
	run_sql --port 3307 "insert into dm_shardddl.t1 values(3,'ccc');"
	run_sql --port 3307 "insert into dm_shardddl.t2 values(4,'ddd');"

	# create job
	job_id=$(create_job "DM" "$CUR_DIR/conf/job.yaml" "dm_shardddl")

	# check data
	exec_with_retry 'run_sql --port 4000 "select count(1) from dm_shardddl.tb\G" | grep -Fq "count(1): 4"'

	# incremental
	run_sql --port 3306 "insert into dm_shardddl.t1 values(5,'eee');"
	run_sql --port 3306 "insert into dm_shardddl.t2 values(6,'fff');"
	run_sql --port 3307 "insert into dm_shardddl.t1 values(7,'ggg');"
	run_sql --port 3307 "insert into dm_shardddl.t2 values(8,'hhh');"

	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | jq -e '.Locks | keys | length == 0'"
	# add column
	run_sql --port 3306 "alter table dm_shardddl.t1 add column new_col1 int;"
	run_sql --port 3306 "alter table dm_shardddl.t2 add column new_col1 int;"
	run_sql --port 3307 "alter table dm_shardddl.t1 add column new_col1 int;"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`t.\` ( \`id\` int(11) NOT NULL, \`name\` varchar(20) DEFAULT NULL, \`new_col1\` int(11) DEFAULT NULL, PRIMARY KEY (\`id\`)' | grep 3"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`t.\` ( \`id\` int(11) NOT NULL, \`name\` varchar(20) DEFAULT NULL, PRIMARY KEY (\`id\`)' | grep 1"
	run_sql --port 3307 "alter table dm_shardddl.t2 add column new_col1 int;"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | jq -e '.Locks | keys | length == 0'"

	run_sql --port 3306 "insert into dm_shardddl.t1 values(9,'iii',9);"
	run_sql --port 3306 "insert into dm_shardddl.t2 values(10,'jjj',10);"
	run_sql --port 3307 "insert into dm_shardddl.t1 values(11,'kkk',11);"
	run_sql --port 3307 "insert into dm_shardddl.t2 values(12,'lll',12);"

	exec_with_retry 'run_sql --port 4000 "select count(1) from dm_shardddl.tb\G" | grep -Fq "count(1): 12"'

	# drop column
	run_sql --port 3306 "alter table dm_shardddl.t1 drop column new_col1;"
	run_sql --port 3306 "alter table dm_shardddl.t2 drop column new_col1;"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`t.\` ( \`id\` int(11) NOT NULL, \`name\` varchar(20) DEFAULT NULL, \`new_col1\` int(11) DEFAULT NULL, PRIMARY KEY (\`id\`)' | grep 2"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`t.\` ( \`id\` int(11) NOT NULL, \`name\` varchar(20) DEFAULT NULL, PRIMARY KEY (\`id\`)' | grep 2"
	run_sql --port 3307 "alter table dm_shardddl.t1 drop column new_col1;"
	run_sql --port 3307 "alter table dm_shardddl.t2 drop column new_col1;"
	run_sql --port 3306 "insert into dm_shardddl.t1 values(13,'mmm');"
	run_sql --port 3306 "insert into dm_shardddl.t2 values(14,'nnn');"
	run_sql --port 3307 "insert into dm_shardddl.t1 values(15,'ooo');"
	run_sql --port 3307 "insert into dm_shardddl.t2 values(16,'ppp');"

	exec_with_retry 'run_sql --port 4000 "select count(1) from dm_shardddl.tb\G" | grep -Fq "count(1): 16"'
	exec_with_retry 'run_sql --port 4000 "SELECT count(1) FROM information_schema.columns WHERE TABLE_SCHEMA =' "'dm_shardddl'" ' AND TABLE_NAME =' "'tb'" '\G" | grep -Fq "count(1): 2"'

	# rename column
	run_sql --port 3306 "alter table dm_shardddl.t1 change column name new_name varchar(20);"
	run_sql --port 3306 "alter table dm_shardddl.t2 change column name new_name varchar(20);"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`t.\` ( \`id\` int(11) NOT NULL, \`name\` varchar(20) DEFAULT NULL, PRIMARY KEY (\`id\`)' | grep 4"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`t.\` ( \`id\` int(11) NOT NULL, \`new_name\` varchar(20) DEFAULT NULL, PRIMARY KEY (\`id\`)' | grep 2"
	run_sql --port 3307 "alter table dm_shardddl.t1 change column name new_name varchar(20);"
	run_sql --port 3307 "alter table dm_shardddl.t2 change column name new_name varchar(20);"

	for ((k = 100; k < 120; k++)); do
		run_sql --port 3306 "insert into dm_shardddl.t1 values(${k},'hahaha');"
		k=$((k + 1))
		run_sql --port 3306 "insert into dm_shardddl.t2 values(${k},'hahaha');"
		k=$((k + 1))
		run_sql --port 3307 "insert into dm_shardddl.t1 values(${k},'hahaha');"
		k=$((k + 1))
		run_sql --port 3307 "insert into dm_shardddl.t2 values(${k},'hahaha');"
		sleep 1
	done

	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | jq -e '.Locks | keys | length == 0'"
	exec_with_retry 'run_sql --port 4000 "select count(1) from dm_shardddl.tb\G" | grep -Fq "count(1): 36"'

	# create table in incremental mode
	run_sql --port 3306 "create table dm_shardddl.new_t1 (id int, name varchar(20), primary key(id)) character set utf8;"
	run_sql --port 3306 "create table dm_shardddl.new_t2 (id int, name varchar(20), primary key(id)) character set utf8;"
	run_sql --port 3307 "create table dm_shardddl.new_t1 (id int, name varchar(20), primary key(id)) character set utf8;"
	run_sql --port 3307 "create table dm_shardddl.new_t2 (id int, name varchar(20), primary key(id)) character set utf8;"
	run_sql --port 3306 "insert into dm_shardddl.new_t1 values(1,'aaa');"
	run_sql --port 3306 "insert into dm_shardddl.new_t2 values(2,'bbb');"
	run_sql --port 3307 "insert into dm_shardddl.new_t1 values(3,'ccc');"
	run_sql --port 3307 "insert into dm_shardddl.new_t2 values(4,'ddd');"

	# partially drop column
	run_sql --port 3306 "alter table dm_shardddl.new_t1 drop column name;"
	run_sql --port 3306 "alter table dm_shardddl.new_t2 drop column name;"
	run_sql --port 3306 "insert into dm_shardddl.new_t1 values(5);"
	run_sql --port 3306 "insert into dm_shardddl.new_t2 values(6);"
	run_sql --port 3307 "insert into dm_shardddl.new_t1 values(7,'eee');"
	run_sql --port 3307 "insert into dm_shardddl.new_t2 values(8,'fff');"
	# add it back
	run_sql --port 3306 "alter table dm_shardddl.new_t1 add column name varchar(20);"
	exec_with_retry --count 30 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id/status\" | tee /dev/stderr | jq -e '.task_status.\"mysql-01\".status.stage == \"Error\" and .task_status.\"mysql-01\".status.result.errors[0].error_code == 36062'"

	# drop one table
	run_sql --port 3307 "drop table dm_shardddl.new_t2;"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`new_t.\` ( \`id\` int(11) NOT NULL, PRIMARY KEY (\`id\`)' | grep 2"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | grep -c 'CREATE TABLE \`new_t.\` ( \`id\` int(11) NOT NULL, \`name\` varchar(20) DEFAULT NULL, PRIMARY KEY (\`id\`)' | grep 1"
	# drop column(name) of remain table
	run_sql --port 3307 "alter table dm_shardddl.new_t1 drop column name;"
	run_sql --port 3307 "insert into dm_shardddl.new_t1 values(9);"
	exec_with_retry 'run_sql --port 4000 "select count(1) from dm_shardddl.new_tb\G" | grep -Fq "count(1): 9"'

	# add name back for all tables
	run_sql --port 3306 "alter table dm_shardddl.new_t2 add column name varchar(20);"
	run_sql --port 3307 "alter table dm_shardddl.new_t1 add column name varchar(20);"
	exec_with_retry --count 30 "curl http://127.0.0.1:10245/api/v1/jobs/$job_id/ddl_locks | tee /dev/stderr | jq -e '.Locks | keys | length == 0'"
	run_sql --port 3306 "insert into dm_shardddl.new_t1 values(10,'ggg');"
	run_sql --port 3306 "insert into dm_shardddl.new_t2 values(11,'hhh');"
	run_sql --port 3307 "insert into dm_shardddl.new_t1 values(12,'iii');"

	curl -X POST "http://127.0.0.1:10245/api/v1/jobs/$job_id/cancel"
	curl -X DELETE "http://127.0.0.1:10245/api/v1/jobs/$job_id"
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
