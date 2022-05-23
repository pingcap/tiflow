#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	run_sql_file $cur/data/db1.dropdb.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK'
	run_sql_file $cur/data/db2.dropdb.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2

	# start DM task only
	dmctl_start_task

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# use sync_diff_inspector to check data now!
	echo "check sync diff for the increment replication"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# restart DM task
	dmctl_stop_task test
	dmctl_start_task

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# use sync_diff_inspector to check data now!
	echo "check sync diff for the increment replication"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "kill dm-worker1"
	kill_process dm-worker1
	echo "kill dm-worker2"
	kill_process dm-worker2

	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ExitAfterDDLBeforeFlush=return(true)'

	run_sql_file $cur/data/db1.increment3.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment3.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# mock recover relay writer
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/relay/MockRecoverRelayWriter=return(true)'

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	check_log_contain_with_retry 'mock recover relay writer' $WORK_DIR/worker2/log/dm-worker.log
	truncate -s 0 $WORK_DIR/worker2/log/dm-worker.log

	run_sql_source1 'insert into sharding22.t1 values(16, "l", 16, 16, 16);'
	run_sql_source2 'insert into sharding22.t1 values(17, "m", 17, 17, 17);'

	# sync without relay
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# flush logs and flush checkpoints
	run_sql_source1 'insert into sharding22.t1 values(18, "o", 18, 18, 18);'
	run_sql_source1 'flush logs;'
	run_sql_source1 'insert into sharding22.t1 values(19, "p", 19, 19, 19);'
	run_sql_source1 'alter table sharding22.t1 add column new_col int;'

	run_sql_source2 'insert into sharding22.t1 values(20, "q", 20, 20, 20);'
	run_sql_source2 'flush logs;'
	run_sql_source2 'insert into sharding22.t1 values(21, "r", 21, 21, 21);'
	run_sql_source2 'alter table sharding22.t1 add column new_col int;'

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# start-relay, purge relay dir and pull from checkpoint
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID2" \
		"\"stage\": \"Running\"" 2

	check_log_contain_with_retry 'will try purge whole relay dir for new relay log' $WORK_DIR/worker2/log/dm-worker.log

	run_sql_source1 'insert into sharding22.t1 values(22, "s", 22, 22, 22, 22);'
	run_sql_source2 'insert into sharding22.t1 values(23, "s", 23, 23, 23, 23);'

	# use sync_diff_inspector to check data now!
	echo "check sync diff for the increment replication"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 3 \
		"\"unit\": \"Sync\"" 2
}

cleanup_data db_target
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
