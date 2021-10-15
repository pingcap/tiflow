#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

API_VERSION="v1alpha1"

function get_mysql_ssl_data_path() {
	run_sql 'SHOW VARIABLES WHERE Variable_Name = "datadir"' $MYSQL_PORT1 $MYSQL_PASSWORD1
	mysql_data_path=$(cat "$TEST_DIR/sql_res.$TEST_NAME.txt" | grep Value | cut -d ':' -f 2 | xargs)
	echo "$mysql_data_path"
}

function run_tidb_with_tls() {
	echo "run a new tidb server with tls"
	cat - >"$WORK_DIR/tidb-tls-config.toml" <<EOF
[status]
status-port = 10090

[security]
# set the path for certificates. Empty string means disabling secure connectoins.
ssl-ca = "$cur/conf/ca.pem"
ssl-cert = "$cur/conf/dm.pem"
ssl-key = "$cur/conf/dm.key"
cluster-ssl-ca = "$cur/conf/ca.pem"
cluster-ssl-cert = "$cur/conf/dm.pem"
cluster-ssl-key = "$cur/conf/dm.key"
EOF

	bin/tidb-server \
		-P 4400 \
		--path $WORK_DIR/tidb \
		--store mocktikv \
		--config $WORK_DIR/tidb-tls-config.toml \
		--log-file "$WORK_DIR/tidb.log" &

	sleep 3
	# if execute failed, print tidb's log for debug
	mysql -uroot -h127.0.0.1 -P4400 --default-character-set utf8 --ssl-ca $cur/conf/ca.pem --ssl-cert $cur/conf/dm.pem --ssl-key $cur/conf/dm.key -E -e "drop database if exists tls" || (cat $WORK_DIR/tidb.log && exit 1)
	mysql -uroot -h127.0.0.1 -P4400 --default-character-set utf8 --ssl-ca $cur/conf/ca.pem --ssl-cert $cur/conf/dm.pem --ssl-key $cur/conf/dm.key -E -e "drop database if exists dm_meta"
}

function prepare_data() {
	run_sql 'DROP DATABASE if exists tls;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE tls;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE tls.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 10); do
		run_sql "INSERT INTO tls.t VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function setup_mysql_tls() {
	mysql_data_path=$(get_mysql_ssl_data_path)
	echo "mysql_ssl_setup at=$mysql_data_path"

	# NOTE we can use ` mysql_ssl_rsa_setup --datadir "$mysql_data_path"` to create a new cert in datadir
	# in ci, mysql in other contianer, so we can't use the mysql_ssl_rsa_setup
	# only mysql 8.0 support use `ALTER INSTANCE RELOAD TLS` to reload cert
	# when use mysql 5.7 we need to restart mysql-server manually if your local server do not enable ssl

	run_sql "grant all on *.* to 'dm_tls_test'@'%' identified by '123456' require ssl;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'flush privileges;' $MYSQL_PORT1 $MYSQL_PASSWORD1

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "s%dir-placeholer%$mysql_data_path%g" $WORK_DIR/source1.yaml
	echo "add dm_tls_test user done $mysql_data_path"
}

function test_worker_ha_when_enable_source_tls() {
	cleanup_data tls
	cleanup_process

	setup_mysql_tls
	run_tidb_with_tls
	prepare_data

	cp $cur/conf/dm-master1.toml $WORK_DIR/
	cp $cur/conf/dm-master2.toml $WORK_DIR/
	cp $cur/conf/dm-master3.toml $WORK_DIR/
	cp $cur/conf/dm-worker1.toml $WORK_DIR/
	cp $cur/conf/dm-worker2.toml $WORK_DIR/
	cp $cur/conf/dm-task.yaml $WORK_DIR/

	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master1.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master2.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master3.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-worker1.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-worker2.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-task.yaml

	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $WORK_DIR/dm-master1.toml
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $WORK_DIR/dm-master2.toml
	run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $WORK_DIR/dm-master3.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

	# operate mysql config to worker
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"operate-source create $WORK_DIR/source1.yaml" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1

	#  start task
	echo "start task and check stage"
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"start-task $WORK_DIR/dm-task.yaml" \
		"\"result\": true" 2

	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"query-status test" \
		"\"result\": true" 2

	echo "check data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "pause task before kill and restart dm-worker"
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"pause-task test" \
		"\"result\": true" 2

	echo "start dm-worker2 and kill dm-worker1"
	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20

	mysql_data_path=$(get_mysql_ssl_data_path)
	echo "mysql_ssl_setup at=$mysql_data_path"

	# change ca.pem name to make sure HA
	mv "$mysql_data_path/ca.pem" "$mysql_data_path/ca.pem.bak"

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $WORK_DIR/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"query-status test" \
		"\"result\": true" 2 \
		"Paused" 1

	# resume task and check stage
	echo "resume task to worker2"
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"resume-task test" \
		"\"result\": true" 2
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"query-status test" \
		"\"result\": true" 2 \
		"worker2" 1

	# incr data
	run_sql 'INSERT INTO tls.t VALUES (99,9999999);' $MYSQL_PORT1 $MYSQL_PASSWORD1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# resume ca.pem
	mv "$mysql_data_path/ca.pem.bak" "$mysql_data_path/ca.pem"

}

function test_master_ha_when_enable_tidb_tls() {
	cleanup_data tls
	cleanup_process

	setup_mysql_tls
	run_tidb_with_tls
	prepare_data

	cp $cur/conf/dm-master1.toml $WORK_DIR/
	cp $cur/conf/dm-master2.toml $WORK_DIR/
	cp $cur/conf/dm-master3.toml $WORK_DIR/
	cp $cur/conf/dm-worker1.toml $WORK_DIR/
	cp $cur/conf/dm-task.yaml $WORK_DIR/

	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master1.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master2.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master3.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-worker1.toml
	sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-task.yaml

	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $WORK_DIR/dm-master1.toml
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $WORK_DIR/dm-master2.toml
	run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $WORK_DIR/dm-master3.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

	# operate mysql config to worker
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"operate-source create $WORK_DIR/source1.yaml" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1

	echo "check master alive"
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"list-member" \
		"\"alive\": true" 3

	echo "start task and check stage"
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"start-task $WORK_DIR/dm-task.yaml" \
		"\"result\": true" 2

	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
		"query-status test" \
		"\"result\": true" 2

	echo "test http and api interface"
	check_rpc_alive $cur/../bin/check_master_online_http 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
	check_rpc_alive $cur/../bin/check_master_http_apis 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

	echo "use common name not in 'cert-allowed-cn' should not request success"
	check_rpc_alive $cur/../bin/check_master_online_http 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/other.pem" "$cur/conf/other.key" && exit 1 || true

	# TODO: curl's version is too low in ci, uncomment this after ci upgrade the version
	#echo "pause task"
	#curl -X PUT --cacert "$cur/conf/ca.pem"  --key "$cur/conf/dm.key" --cert "$cur/conf/dm.pem" https://127.0.0.1:$MASTER_PORT1/apis/$API_VERSION/tasks/test -d '{"op": 2}' > $WORK_DIR/pause.log || cat $WORK_DIR/pause.log
	#check_log_contains $WORK_DIR/pause.log "\"result\": true" 2

	#echo "query status"
	#curl -X GET --cacert "$cur/conf/ca.pem"  --key "$cur/conf/dm.key" --cert "$cur/conf/dm.pem" https://127.0.0.1:$MASTER_PORT1/apis/$API_VERSION/status/test > $WORK_DIR/status.log || cat $WORK_DIR/status.log
	#check_log_contains $WORK_DIR/status.log "\"stage\": \"Paused\"" 1

	echo "check data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# https://github.com/pingcap/dm/issues/1458
	check_log_not_contains $WORK_DIR/master1/log/dm-master.log "remote error: tls: bad certificate"
}

function run() {
	test_worker_ha_when_enable_source_tls
	test_master_ha_when_enable_tidb_tls
}

cleanup_data tls
cleanup_process

run $*

# kill the tidb with tls
pkill -hup tidb-server 2>/dev/null || true
wait_process_exit tidb-server

run_tidb_server 4000 $TIDB_PASSWORD

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
