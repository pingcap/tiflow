#!/bin/bash

set -eu

export TEST_DIR=/tmp/dm_test
export TEST_NAME="mariadb_master_down_and_up"
export DM_MASTER_EXTRA_ARG=""

WORK_DIR=$TEST_DIR/$TEST_NAME
rm -rf $WORK_DIR
mkdir -p $WORK_DIR

db="db_pessimistic"
tb="tb"

master_port="3306"
slave_port="3307"
tidb_port="4000"
MASTER_PORT=8261
WORKER1_PORT=8262

function exec_sql() {
	echo $2 | MYSQL_PWD=123456 mysql -uroot -h127.0.0.1 -P$1
}

function exec_tidb() {
	echo $2 | mysql -uroot -h127.0.0.1 -P$1
}

function install_sync_diff() {
	curl https://download.pingcap.org/tidb-enterprise-tools-nightly-linux-amd64.tar.gz | tar xz
	mkdir -p bin
	mv tidb-enterprise-tools-nightly-linux-amd64/bin/sync_diff_inspector bin/
}

function get_master_status() {
	arr=$(echo "show master status;" | MYSQL_PWD=123456 mysql -uroot -h127.0.0.1 -P3306 | awk 'NR==2')
	echo $arr
}

function change_master_to_gtid() {
	exec_sql $1 "stop slave;"
	exec_sql $1 "change master to master_host='mariadb_master',master_port=$2,master_user='root',master_password='123456',master_use_gtid=slave_pos;"
	exec_sql $1 "start slave;"
}

function wait_mysql() {
	echo "-------wait_mysql--------"

	i=0
	while ! mysqladmin -h127.0.0.1 -P$1 -uroot ping --connect-timeout=1 >/dev/null 2>&1; do
		echo "wait mysql"
		i=$((i + 1))
		if [ "$i" -gt 20 ]; then
			echo "wait for mysql $1:3306 timeout"
			exit 1
		fi
		sleep 1
	done
	i=0

	server_id=$(echo "show variables like 'server_id';" | MYSQL_PWD=123456 mysql -uroot -h127.0.0.1 -P$1 | awk 'NR==2' | awk '{print $2}')
	while [ "$server_id" != $2 ]; do
		echo "wait server_id"
		i=$((i + 1))
		if [ "$i" -gt 20 ]; then
			echo "different server_id: $server_id, expect: $2, host: $1"
			exit 1
		fi
		sleep 1
		server_id=$(echo "show variables like 'server_id';" | MYSQL_PWD=123456 mysql -uroot -h127.0.0.1 -P$1 | awk 'NR==2' | awk '{print $2}')
	done
}
