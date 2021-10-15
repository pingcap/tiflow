#!/bin/bash

set -eu

export TEST_DIR=/tmp/dm_test
export TEST_NAME="upstream_switch"

WORK_DIR=$TEST_DIR/$TEST_NAME
rm -rf $WORK_DIR
mkdir -p $WORK_DIR

db="db_pessimistic"
tb="tb"
host1="172.28.128.2"
host2="172.28.128.3"
master_57_host="172.28.128.4"
slave_57_host="172.28.128.5"
master_8_host="172.28.128.6"
slave_8_host="172.28.128.7"
tidb_host="172.28.128.8"
MASTER_PORT=8261
WORKER1_PORT=8262
WORKER2_PORT=8263

function exec_sql() {
	echo $2 | MYSQL_PWD=123456 mysql -uroot -h$1 -P3306
}

function exec_tidb() {
	echo $2 | mysql -uroot -h$1 -P4000
}

function install_sync_diff() {
	curl http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz | tar xz
	mkdir -p bin
	mv tidb-enterprise-tools-latest-linux-amd64/bin/sync_diff_inspector bin/
}

function prepare_more_binlogs() {
	exec_sql $1 "create database db1;"
	exec_sql $1 "flush logs;"
	exec_sql $1 "create table db1.tb1(id int);"
	exec_sql $1 "flush logs;"
	exec_sql $1 "insert into db1.tb1 values(1);"
	exec_sql $1 "insert into db1.tb1 values(2),(3),(4);"
}

function prepare_less_binlogs() {
	exec_sql $1 "create database db2;"
	exec_sql $1 "flush logs;"
	exec_sql $1 "create table db2.tb2(id int);"
	exec_sql $1 "insert into db2.tb2 values(1);"
	exec_sql $1 "insert into db2.tb2 values(2),(3);"
}

function get_master_status() {
	arr=$(echo "show master status;" | MYSQL_PWD=123456 mysql -uroot -h$1 -P3306 | awk 'NR==2')
	echo $arr
}

function change_master_to_pos() {
	exec_sql $1 "stop slave;"
	echo "change master to master_host='$2',master_user='root',master_password='123456',master_log_file='$3',master_log_pos=$4;"
	exec_sql $1 "change master to master_host='$2',master_user='root',master_password='123456',master_log_file='$3',master_log_pos=$4;"
	exec_sql $1 "start slave;"
}

function change_master_to_gtid() {
	exec_sql $1 "stop slave;"
	exec_sql $1 "change master to master_host='$2',master_user='root',master_password='123456',master_auto_position=1;"
	exec_sql $1 "start slave;"
}

function wait_mysql() {
	echo "-------wait_mysql--------"

	i=0
	while ! mysqladmin -h$1 -P3306 -uroot ping --connect-timeout=1 >/dev/null 2>&1; do
		echo "wait mysql"
		i=$((i + 1))
		if [ "$i" -gt 20 ]; then
			echo "wait for mysql $1:3306 timeout"
			exit 1
		fi
		sleep 1
	done
	i=0

	server_id=$(echo "show variables like 'server_id';" | MYSQL_PWD=123456 mysql -uroot -h$1 -P3306 | awk 'NR==2' | awk '{print $2}')
	while [ "$server_id" != $2 ]; do
		echo "wait server_id"
		i=$((i + 1))
		if [ "$i" -gt 20 ]; then
			echo "different server_id: $server_id, expect: $2, host: $1"
			exit 1
		fi
		sleep 1
		server_id=$(echo "show variables like 'server_id';" | MYSQL_PWD=123456 mysql -uroot -h$1 -P3306 | awk 'NR==2' | awk '{print $2}')
	done
}
