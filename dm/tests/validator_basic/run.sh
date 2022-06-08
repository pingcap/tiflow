#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

db_name=$TEST_NAME

function prepare_for_standalone_test() {
	cleanup_process $*
	cleanup_data $db_name
	cleanup_data_upstream $db_name

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	cp $cur/conf/dm-task-standalone.yaml $WORK_DIR/dm-task.yaml
	dmctl_start_task_standalone $WORK_DIR/dm-task.yaml --remove-meta

	# sync-diff seems cannot handle float/double well, will skip it here
}

<<<<<<< HEAD
function run_standalone() {
	echo "--> normal case, check we validate different data types"
=======
function trigger_checkpoint_flush() {
	sleep 1.5
	run_sql_source1 "alter table $db_name.t1 comment 'a';" # force flush checkpoint
}

function restore_timezone() {
	echo "restore time_zone"
	run_sql_source1 "set global time_zone = SYSTEM"
	run_sql_tidb "set global time_zone = SYSTEM"
}

function restore_on_case_exit() {
	echo "restore config"
	mv $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	mv $cur/conf/source1.yaml.bak $cur/conf/source1.yaml

	restore_timezone
}

function test_validator_together_with_task() {
	enable_gtid=$1
	enable_relay=$2
	# clear fail point
	export GO_FAILPOINTS=""
	echo "--> full mode, check we validate different data types(gtid=$enable_gtid, relay=$enable_relay)"
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	cp $cur/conf/source1.yaml.bak $cur/conf/source1.yaml
	sed -i 's/enable-gtid: false/enable-gtid: '$enable_gtid'/g' $cur/conf/source1.yaml
	sed -i 's/enable-relay: false/enable-relay: '$enable_relay'/g' $cur/conf/source1.yaml
>>>>>>> 8a1862057 (test(dm): fix unstable validator case (#5773))
	prepare_for_standalone_test

	# key=6, mysql store as 1.2345679e+17, but in tidb it's 1.23457e17
	# so will fail in current compare rule
	# note: key=1 has the same condition, but it's not validated, since it's migrated in full phase.
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 6"
<<<<<<< HEAD
=======
}

function test_start_validator_on_the_fly_prepare() {
	enable_gtid=$1
	enable_relay=$2
	# clear fail point
	export GO_FAILPOINTS=""
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	cp $cur/conf/source1.yaml.bak $cur/conf/source1.yaml
	sed -i 's/enable-gtid: false/enable-gtid: '$enable_gtid'/g' $cur/conf/source1.yaml
	sed -i 's/enable-relay: false/enable-relay: '$enable_relay'/g' $cur/conf/source1.yaml
	run_sql_source1 "flush binary logs"
	sleep 1
	run_sql_source1 "purge binary logs before now()"
	prepare_dm_and_source
	dmctl_start_task_standalone $cur/conf/dm-task-standalone-no-validator.yaml --remove-meta
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config source $SOURCE_ID1" \
		"enable-gtid: $enable_gtid" 1 \
		"enable-relay: $enable_relay" 1
	run_sql_source1 "insert into $db_name.t1(id, name) values(2,'b'), (3, 'c')"
	run_sql_source1 "delete from $db_name.t1 where id=1"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator not found for task" 1
}

function test_start_validator_on_the_fly() {
	enable_gtid=$1
	enable_relay=$2
	echo "--> start validator on the fly, validate from current syncer progress(gtid=$enable_gtid, relay=$enable_relay)"
	test_start_validator_on_the_fly_prepare $enable_gtid $enable_relay
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/0\"" 1
	run_sql_source1 "insert into $db_name.t1(id, name) values(4,'d'), (5, 'e')"
	run_sql_source1 "update $db_name.t1 set name='bb' where id=2"
	run_sql_source1 "delete from $db_name.t1 where id=3"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 2\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function test_start_validator_with_time_smaller_than_min_binlog_pos() {
	enable_gtid=$1
	echo "--> start validator from time < min mysql binlog pos(gtid=$enable_gtid)"
	test_start_validator_on_the_fly_prepare $enable_gtid false
	run_sql_source1 "insert into $db_name.t1(id, name) values(4,'d'), (5, 'e')"
	run_sql_source1 "update $db_name.t1 set name='bb' where id=2"
	run_sql_source1 "delete from $db_name.t1 where id=3"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time '$(($(date "+%Y") - 2))-01-01 00:00:00' test" \
		"\"result\": true" 1
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 5\/1\/2\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function test_start_validator_with_time_in_range_of_binlog_pos() {
	enable_gtid=$1
	echo "--> start validator from time which is in range of mysql binlog(gtid=$enable_gtid)"
	test_start_validator_on_the_fly_prepare $enable_gtid false
	run_sql_source1 "insert into $db_name.t1(id, name) values(4,'d'), (5, 'e')"
	run_sql_source1 "update $db_name.t1 set name='bb' where id=2"
	run_sql_source1 "delete from $db_name.t1 where id=3"
	run_sql_source1 "insert into $db_name.t1(id, name) values(100,'d'), (101, 'e')"
	sleep 2
	start_time="$(TZ='UTC-2' date '+%Y-%m-%d %T')" # TZ=UTC-2 means +02:00
	run_sql_source1 "insert into $db_name.t1(id, name) values(200,'d'), (201, 'e')"
	run_sql_source1 "delete from $db_name.t1 where id=200"
	run_sql_source1 "update $db_name.t1 set name='dd' where id=100"
	run_sql_source1 "insert into $db_name.t1(id, name) values(300,'d')"
	run_sql_source1 "update $db_name.t1 set name='ee' where id=101"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time '$start_time' test" \
		"\"result\": true" 1
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/2\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function run_standalone() {
	#
	# we have set row-error-delay and meta-flush-interval small enough for cases below,
	# but it maybe unstable if validator reached progress of syncer before the last ddl,
	# and then validator start to mark failed rows as error. So we may have more errors
	# than expected.
	#

	# backup it, will change it in the following case
	cp $cur/conf/dm-task-standalone.yaml $cur/conf/dm-task-standalone.yaml.bak
	cp $cur/conf/source1.yaml $cur/conf/source1.yaml.bak

	trap restore_on_case_exit EXIT

	# use different timezone for upstream and tidb
	run_sql_source1 "set global time_zone = '+02:00'"
	run_sql_source1 "SELECT cast(TIMEDIFF(NOW(6), UTC_TIMESTAMP(6)) as time) time"
	check_contains "time: 02:00:00"
	run_sql_tidb "set global time_zone = '+06:00'"
	run_sql_tidb "SELECT cast(TIMEDIFF(NOW(6), UTC_TIMESTAMP(6)) as time) time"
	check_contains "time: 06:00:00"

	test_validator_together_with_task false false
	test_validator_together_with_task false true
	test_validator_together_with_task true false
	test_validator_together_with_task true true

	echo "--> fast mode, check we validate different data types"
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	sed -i 's/    mode: full/    mode: fast/' $cur/conf/dm-task-standalone.yaml
	prepare_for_standalone_test
	# in fast mode we don't check col by col, so key=6 will pass
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 6"
>>>>>>> 8a1862057 (test(dm): fix unstable validator case (#5773))

	echo "--> check we can catch inconsistent rows"
	# skip incremental rows with id <= 5
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(5)'
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 6\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 3"

	echo "--> check update pk(split into insert and delete)"
	run_sql_source1 "update $db_name.t1 set id=100 where id=7"
	run_sql_source1 "alter table $db_name.t1 comment 'a';" # force flush checkpoint
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 7\/1\/2\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 6\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 3"

	echo "--> check validator panic and we can catch it"
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ValidatorPanic=panic("validator panic")'
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator panic" 1

	echo "--> check validator worker panic and we can catch it"
	# panic 1 times
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ValidatorWorkerPanic=1*panic("validator worker panic")'
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	sleep 5
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator worker panic" 1

	# in real world panic, we cannot restart just like this, this case only makes sure worker panic doesn't
	# mess status of validator up
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 6"
}

run_standalone $*
cleanup_process $*
cleanup_data $db_name
cleanup_data_upstream $db_name

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
