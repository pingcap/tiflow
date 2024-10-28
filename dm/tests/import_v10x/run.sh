#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	# load some data both into upstream and downstream.
	run_sql_both_source "drop database if exists import_v10x"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	run_sql_tidb "drop database if exists import_v10x"
	run_sql_file $cur/data/db1.prepare.sql 127.0.0.1 $TIDB_PORT $TIDB_PASSWORD
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql 127.0.0.1 $TIDB_PORT $TIDB_PASSWORD
	check_contains 'Query OK, 3 rows affected'

	# NOTE: to run a real v1.0.x cluster is a little bit complicated in CI.
	# generate checkpoint data for v1.0.x.
	run_sql_source1 "SHOW MASTER STATUS"
	binlog_name1=$(grep "File" "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk -F: '{print $2}' | tr -d ' ')
	binlog_pos1=$(grep "Position" "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk -F: '{print $2}' | tr -d ' ')
	binlog_gtid1=$(grep "Executed_Gtid_Set" "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk -F ': ' '{print $2}' | tr -d ' ')
	echo "source1 master status", $binlog_name1, $binlog_pos1, $binlog_gtid1

	run_sql_source2 "SHOW MASTER STATUS"
	binlog_name2=$(grep "File" "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk -F: '{print $2}' | tr -d ' ')
	binlog_pos2=$(grep "Position" "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk -F: '{print $2}' | tr -d ' ')
	binlog_gtid2=$(grep "Executed_Gtid_Set" "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk -F ': ' '{print $2}' | tr -d ' ')
	echo "source2 master status", $binlog_name2, $binlog_pos2, $binlog_gtid2

	cp $cur/data/v106_syncer_checkpoint.sql $WORK_DIR/v106_syncer_checkpoint.sql
	sed -i "s/BINLOG_NAME1/$binlog_name1/g" $WORK_DIR/v106_syncer_checkpoint.sql
	sed -i "s/BINLOG_POS1/$binlog_pos1/g" $WORK_DIR/v106_syncer_checkpoint.sql
	sed -i "s/BINLOG_NAME2/$binlog_name2/g" $WORK_DIR/v106_syncer_checkpoint.sql
	sed -i "s/BINLOG_POS2/$binlog_pos2/g" $WORK_DIR/v106_syncer_checkpoint.sql

	run_sql_file $WORK_DIR/v106_syncer_checkpoint.sql 127.0.0.1 $TIDB_PORT $TIDB_PASSWORD

	# generate/copy dm_worker_meta for v1.0.x.
	mkdir -p $WORK_DIR/worker1
	mkdir -p $WORK_DIR/worker2
	cp -r $cur/dm_worker1_meta $WORK_DIR/worker1/dm_worker_meta
	cp -r $cur/dm_worker2_meta $WORK_DIR/worker2/dm_worker_meta

	# put source config files into `v1-sources-path`.
	mkdir -p $WORK_DIR/master/v1-sources
	cp $cur/conf/source1.yaml $WORK_DIR/master/v1-sources/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/master/v1-sources/source2.yaml

	# prepare key file for dm-master.
	mkdir -p $WORK_DIR/master
	cp $cur/conf/key.txt $WORK_DIR/master/

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# check source created.
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source show" \
		"\"result\": true" 3 \
		'"worker": "worker1"' 1 \
		'"worker": "worker2"' 1

	# check task running.
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	# check task config, just a simple match
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config task test --path $WORK_DIR/task.yaml" \
		"\"result\": true" 1

	sed -i "s/password: '\*\*\*\*\*\*'/password: \"\"/g" $WORK_DIR/task.yaml
	diff $cur/conf/task.yaml $WORK_DIR/task.yaml || exit 1

	run_sql "show create table \`dm_meta\`.\`test_syncer_checkpoint\`" $TIDB_PORT $TIDB_PASSWORD
	check_contains "\`exit_safe_binlog_name\` varchar(128) DEFAULT ''"
	# different TiDB will output DEFAULT 0 or DEFAULT '0'
	check_contains "\`exit_safe_binlog_pos\` int"
	check_contains "\`exit_safe_binlog_gtid\` text DEFAULT NULL"

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# use sync_diff_inspector to check data now!
	sleep 2
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data import_v10x
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
