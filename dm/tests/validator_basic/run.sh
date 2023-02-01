#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

db_name=$TEST_NAME

function prepare_dm_and_source() {
	cleanup_process $*
	cleanup_data $db_name
	cleanup_data_upstream $db_name

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# source 1 should be bound to worker 1
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
}

function prepare_for_standalone_test() {
	prepare_dm_and_source
	dmctl_start_task_standalone $cur/conf/dm-task-standalone.yaml --remove-meta

	# sync-diff seems cannot handle float/double well, will skip it here
}

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
	prepare_for_standalone_test
	# key=6, mysql store as 1.2345679e+17, but in tidb it's 1.23457e17
	# so will fail in current compare rule
	# note: key=1 has the same condition, but it's not validated, since it's migrated in full phase.
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config source $SOURCE_ID1" \
		"enable-gtid: $enable_gtid" 1 \
		"enable-relay: $enable_relay" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1
	run_sql_tidb "SELECT count(*) from $db_name.t1_down"
	check_contains "count(*): 6"
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

	# validator is started together with task
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
	run_sql_tidb "SELECT count(*) from $db_name.t1_down"
	check_contains "count(*): 6"

	echo "--> check we can catch inconsistent rows: full mode"
	# skip incremental rows with id <= 5
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(5)'
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# 6 inconsistent rows:
	# insert of id = 3, 4, 5 are skipped.
	# insert & update of id = 2 are merged and skipped
	# delete of id = 1 are skipped
	# id = 6, float precision problem, so inconsistent
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 6\/0\/0" 1
	run_sql_tidb "SELECT count(*) from $db_name.t1_down"
	check_contains "count(*): 3"

	echo "--> check we can catch inconsistent rows: fast mode"
	# skip incremental rows with id <= 5
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(5)'
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	sed -i 's/    mode: full/    mode: fast/' $cur/conf/dm-task-standalone.yaml
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# 5 inconsistent rows, nearly same as previous test, but id = 6 success in fast mode
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 5\/0\/0" 1
	run_sql_tidb "SELECT count(*) from $db_name.t1_down"
	check_contains "count(*): 3"

	echo "--> check update pk(split into insert and delete)"
	run_sql_source1 "update $db_name.t1 set id=100 where id=7"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 7\/1\/2\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 5\/0\/0" 1
	run_sql_tidb "SELECT count(*) from $db_name.t1_down"
	check_contains "count(*): 3"

	echo "--> check validator panic and we can catch it"
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ValidatorPanic=panic("validator panic")'
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator panic" 1

	echo "--> check validator worker panic and we can catch it"
	# panic 1 times
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ValidatorWorkerPanic=1*panic("validator worker panic")'
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
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
	run_sql_tidb "SELECT count(*) from $db_name.t1_down"
	check_contains "count(*): 6"

	echo "--> check validator stop when pending row size too large"
	# skip incremental rows with id <= 5
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(5)'
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	sed -i 's/row-error-delay: .*$/row-error-delay: 30m/' $cur/conf/dm-task-standalone.yaml
	sed -i 's/max-pending-row-size: .*$/max-pending-row-size: 20/' $cur/conf/dm-task-standalone.yaml
	prepare_for_standalone_test
	run_sql_source1 "create table $db_name.t1_large_col(id int primary key, c varchar(100))"
	run_sql_source1 "insert into $db_name.t1_large_col values(1, 'this-text-is-more-than-20-bytes')"
	trigger_checkpoint_flush
	# since multiple worker may send this error, there should be at least one "too much pending data" error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"stage\": \"Stopped\"" 1 \
		"too much pending data, stop validator" 1+ \
		"\"processedRowsStatus\": \"insert\/update\/delete: 1\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 1\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1

	echo "--> check validator stop when pending row count too many"
	# skip incremental rows with id <= 5
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(5)'
	cp $cur/conf/dm-task-standalone.yaml.bak $cur/conf/dm-task-standalone.yaml
	sed -i 's/row-error-delay: .*$/row-error-delay: 30m/' $cur/conf/dm-task-standalone.yaml
	sed -i 's/max-pending-row-count: .*$/max-pending-row-count: 2/' $cur/conf/dm-task-standalone.yaml
	prepare_for_standalone_test
	run_sql_source1 "create table $db_name.t1_large_col(id int primary key)"
	run_sql_source1 "insert into $db_name.t1_large_col values(1)"
	run_sql_source1 "insert into $db_name.t1_large_col values(2)"
	run_sql_source1 "insert into $db_name.t1_large_col values(3)"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"stage\": \"Stopped\"" 1 \
		"too much pending data, stop validator" 1+ \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 3\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1

	test_start_validator_on_the_fly false false
	test_start_validator_on_the_fly false true
	test_start_validator_on_the_fly true false
	test_start_validator_on_the_fly true true

	test_start_validator_with_time_smaller_than_min_binlog_pos false
	test_start_validator_with_time_smaller_than_min_binlog_pos true

	test_start_validator_with_time_in_range_of_binlog_pos false
	test_start_validator_with_time_in_range_of_binlog_pos true

	echo "--> start validator from time > max mysql binlog pos"
	test_start_validator_on_the_fly_prepare false false
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time '$(($(date "+%Y") + 2))-01-01 00:00:00' test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"is too late, no binlog location matches it" 1 \
		"\"stage\": \"Stopped\"" 1 \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
}

function validate_table_with_different_pk() {
	export GO_FAILPOINTS=""

	# int type uk already tested in run_standalone, so not test it here
	echo "--> single varchar col pk"
	prepare_for_standalone_test
	run_sql_source1 "create table $db_name.t2(c1 varchar(10) primary key, c2 varchar(10))"
	run_sql_source1 "insert into $db_name.t2 values('a', NULL), ('b', 'b'), ('c', 'c')"
	run_sql_source1 "update $db_name.t2 set c2='bb' where c1='b'"
	run_sql_source1 "update $db_name.t2 set c2='cc' where c1='c'"
	run_sql_source1 "delete from $db_name.t2 where c1='a'"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/2\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
	run_sql_tidb "SELECT count(*) from $db_name.t2"
	check_contains "count(*): 2"

	echo "--> single datetime col pk"
	prepare_for_standalone_test
	run_sql_source1 "create table $db_name.t2(c1 datetime primary key, c2 varchar(10))"
	run_sql_source1 "insert into $db_name.t2 values('2022-01-01 00:00:00', NULL), ('2022-01-01 00:00:01', 'b'), ('2022-01-01 00:00:02', 'c')"
	run_sql_source1 "update $db_name.t2 set c2='bb' where c1='2022-01-01 00:00:01'"
	run_sql_source1 "update $db_name.t2 set c2='cc' where c1='2022-01-01 00:00:02'"
	run_sql_source1 "delete from $db_name.t2 where c1='2022-01-01 00:00:00'"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/2\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
	run_sql_tidb "SELECT count(*) from $db_name.t2"
	check_contains "count(*): 2"

	echo "--> compound pk (datetime, timestamp, int, varchar)"
	prepare_for_standalone_test
	run_sql_source1 "create table $db_name.t2(c1 datetime, c2 timestamp DEFAULT CURRENT_TIMESTAMP, c3 int, c4 varchar(10), c5 int, primary key(c1, c2, c3, c4))"
	run_sql_source1 "insert into $db_name.t2 values('2022-01-01 00:00:00', '2022-01-01 00:00:00', 1, 'a', 1)"
	run_sql_source1 "insert into $db_name.t2 values('2022-01-01 00:00:00', '2022-01-01 00:00:00', 1, 'b', 2)"
	run_sql_source1 "insert into $db_name.t2 values('2022-01-01 00:00:00', '2012-12-01 00:00:00', 1, 'a', 3)"
	run_sql_source1 "insert into $db_name.t2 values('2012-12-01 00:00:00', '2022-01-01 00:00:00', 1, 'a', 4)"
	run_sql_source1 "update $db_name.t2 set c5=11 where c1='2022-01-01 00:00:00'" # update 3 rows
	run_sql_source1 "update $db_name.t2 set c5=22 where c2='2012-12-01 00:00:00'" # update 1 row
	run_sql_source1 "delete from $db_name.t2 where c4='a'"                        # delete 3 row
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 4\/4\/3\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
	run_sql_tidb "SELECT count(*) from $db_name.t2"
	check_contains "count(*): 1"
}

function test_unsupported_table_status() {
	export GO_FAILPOINTS=""

	echo "--> table without primary key"
	prepare_for_standalone_test
	run_sql_source1 "create table $db_name.t2(c1 int)"
	run_sql_source1 "insert into $db_name.t2 values(1)"
	trigger_checkpoint_flush
	# skipped row is not add to processed rows
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 1 \
		"no primary key" 1
	run_sql_tidb "SELECT count(*) from $db_name.t2"
	check_contains "count(*): 1"

	echo "--> table is deleted on downstream"
	prepare_dm_and_source
	run_sql_source1 "reset master"
	dmctl_start_task_standalone $cur/conf/dm-task-standalone-no-validator.yaml --remove-meta
	run_sql_source1 "create table $db_name.t2(c1 int primary key, c2 int, c3 int)"
	run_sql_source1 "insert into $db_name.t2 values(1, 1, 1)"
	run_sql_source1 "insert into $db_name.t2 values(2, 2, 2)"
	run_sql_source1 "drop table $db_name.t2"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1
	run_sql_tidb "select count(*) from information_schema.tables where TABLE_SCHEMA='${db_name}' and TABLE_NAME = 't2'"
	check_contains "count(*): 0"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time '$(($(date "+%Y") - 2))-01-01 00:00:00' test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 1 \
		"table is not synced or dropped" 1

	echo "--> table in schema-tracker has less column than binlog"
	prepare_dm_and_source
	run_sql_source1 "reset master"
	dmctl_start_task_standalone $cur/conf/dm-task-standalone-no-validator.yaml --remove-meta
	run_sql_source1 "create table $db_name.t2(c1 int primary key, c2 int, c3 int)"
	run_sql_source1 "insert into $db_name.t2 values(1, 1, 1)"
	run_sql_source1 "alter table $db_name.t2 drop column c3"
	run_sql_source1 "insert into $db_name.t2 values(2, 2)"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1
	run_sql_tidb "SELECT count(*) from $db_name.t2"
	check_contains "count(*): 2"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time '$(($(date "+%Y") - 2))-01-01 00:00:00' test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 1 \
		"binlog has more columns than current table" 1

	echo "--> pk column of downstream table not in range of binlog column"
	prepare_dm_and_source
	run_sql_source1 "reset master"
	dmctl_start_task_standalone $cur/conf/dm-task-standalone-no-validator.yaml --remove-meta
	run_sql_source1 "create table $db_name.t2(c1 int)"
	run_sql_source1 "insert into $db_name.t2 values(1)"
	run_sql_source1 "alter table $db_name.t2 add column c2 int default 1"
	run_sql_source1 "alter table $db_name.t2 add primary key(c2)"
	run_sql_source1 "insert into $db_name.t2 values(2, 2)"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1
	run_sql_tidb "SELECT count(*) from $db_name.t2"
	check_contains "count(*): 2"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time '$(($(date "+%Y") - 2))-01-01 00:00:00' test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 1 \
		"primary key column of downstream table out of range of binlog event row" 1
}

function stopped_validator_fail_over() {
	export GO_FAILPOINTS=""

	echo "--> stopped validator fail over"
	# skip incremental rows with c1 <= 1
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(1)'
	prepare_for_standalone_test
	run_sql_source1 "create table $db_name.t2(c1 int primary key, c2 int)"
	run_sql_source1 "insert into $db_name.t2 values(1, 1), (2, 2), (3, 3)"
	run_sql_source1 "update $db_name.t2 set c2=11 where c2=1"
	run_sql_source1 "update $db_name.t2 set c2=22 where c2=2"
	run_sql_source1 "delete from $db_name.t2 where c1=3"
	trigger_checkpoint_flush
	# skipped row is not add to processed rows
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/2\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1 \
		"\"stage\": \"Running\"" 2
	# make sure validator checkpoint is flushed
	sleep 1 # wait for the min flush interval
	trigger_checkpoint_flush
	run_sql_tidb_with_retry "select concat_ws('/', procd_ins, procd_upd, procd_del) processed
														from dm_meta.test_validator_checkpoint where source='mysql-replica-01'" \
		"processed: 3/2/1"
	run_sql_tidb_with_retry "select count(1) cnt
														from dm_meta.test_validator_error_change where source='mysql-replica-01'" \
		"cnt: 1"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/2\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1 \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 1
	# source1 is bound to worker1, so source1 will be bound to worker2. see prepare_dm_and_source
	kill_process worker1
	# stopped task fail over, processed row status and table status is not loaded into memory, so they're zero
	# but we can see errors, since it's loaded from db all the time
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1 \
		"\"stage\": \"Running\"" 0 \
		"\"stage\": \"Stopped\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error all test" \
		"\"result\": true" 1 \
		"\"id\": \"1\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1
	run_sql_source1 "insert into $db_name.t2 values(4, 4), (5, 5), (6, 6)"
	run_sql_source1 "update $db_name.t2 set c2=55 where c2=5"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/3\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1 \
		"\"stage\": \"Running\"" 2
}

# some events are filtered in syncer, validator should filter them too.
# validator only support below 3 filter.
function test_data_filter() {
	export GO_FAILPOINTS=""

	echo "--> filter online ddl shadow table"
	prepare_dm_and_source
	dmctl_start_task_standalone $cur/conf/test-filter.yaml --remove-meta
	run_sql_source1 "create table $db_name.t2(id int primary key)"
	run_sql_source1 "insert into $db_name.t2 values(1), (2), (3)"
	run_sql_source1 "create table $db_name._t_gho(id int primary key)"
	run_sql_source1 "insert into $db_name._t_gho values(1), (2), (3)"
	trigger_checkpoint_flush
	# skipped table has no table status, so number of running = 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"\"stage\": \"Running\"" 2

	echo "--> filter by ba list"
	run_sql_source1 "create table $db_name.x1(id int primary key)"
	run_sql_source1 "insert into $db_name.x1 values(1), (2), (3)"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1
	# skipped table has no table status, so number of running is still 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"\"stage\": \"Running\"" 2

	echo "--> filter by filter-rules"
	run_sql_source1 "create table $db_name.t_filter_del(id int primary key, val int)"
	run_sql_source1 "insert into $db_name.t_filter_del values(1, 1), (2, 2), (3, 3), (4, 4)"
	run_sql_source1 "update $db_name.t_filter_del set val=11 where id=1"
	run_sql_source1 "update $db_name.t_filter_del set val=22 where id=2"
	run_sql_source1 "delete from $db_name.t_filter_del where id in (1, 2, 3)"
	trigger_checkpoint_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1
	run_sql_tidb "SELECT count(*) from $db_name.t_filter_del"
	check_contains "count(*): 4"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 7\/2\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"\"stage\": \"Running\"" 3
}

function test_validation_syncer_stopped() {
	echo "--> validate when syncer is stopped"
	insertCnt=5
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/mockValidatorDelay=return(2)"
	prepare_for_standalone_test
	run_sql_source1 "create table validator_basic.test(a int primary key, b int)"
	run_sql_source1 "insert into validator_basic.test values(0, 0)"
	trigger_checkpoint_flush
	# wait syncer to start so that validator can start
	sleep 4
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1 \
		"\"processedRowsStatus\": \"insert\/update\/delete: 1\/0\/0\"" 1
	for ((k = 1; k <= $insertCnt; k++)); do
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"pause-task test" \
			"\"result\": true" 2
		trigger_checkpoint_flush
		# catchup the last insert when the syncer is stopped
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"processedRowsStatus\": \"insert\/update\/delete: $k\/0\/0\"" 1 \
			"new\/ignored\/resolved: 0\/0\/0" 1
		run_sql_source1 "insert into validator_basic.test values($k, $k)"
		trigger_checkpoint_flush
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"resume-task test" \
			"\"result\": true" 2
		# syncer synced but the validator delayed
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"synced\": true" 1 \
			"\"processedRowsStatus\": \"insert\/update\/delete: $k\/0\/0\"" 1
	done
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: $(($insertCnt + 1))\/0\/0\"" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1
}

function test_dup_autopk() {
	# successfully validate sharding tables
	# though the downstream table deletes the primary key
	# because not null unique key can identify a row accurately.
	echo "--> test duplicate auto-incr pk"
	export GO_FAILPOINTS=""
	prepare_dm_and_source
	dmctl_operate_source create $cur/conf/source2.yaml $SOURCE_ID2
	# auto_incr pk in both tables
	run_sql_source1 "create table validator_basic.shard1(id int primary key auto_increment, ukey int not null unique key)"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD1
	run_sql_source2 "create table validator_basic.shard1(id int primary key auto_increment, ukey int not null unique key)"
	# delete pk in tidb
	run_sql_tidb "create database if not exists validator_basic"
	run_sql_tidb "create table validator_basic.shard(id int, ukey int not null unique key)"
	dmctl_start_task "$cur/conf/sharding-task.yaml" "--remove-meta"
	run_sql_source1 "insert into validator_basic.shard1(ukey) values(1),(2),(3)"
	run_sql_source2 "insert into validator_basic.shard1(ukey) values(4),(5),(6)"
	sleep 1.5
	run_sql_source2 "alter table $db_name.t1 comment 'a';" # trigger source2 flush
	trigger_checkpoint_flush                               # trigger source1 flush
	# validate pass
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 3\/0\/0\"" 2 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 2
}

run_standalone $*
validate_table_with_different_pk
test_unsupported_table_status
stopped_validator_fail_over
test_data_filter
test_validation_syncer_stopped
test_dup_autopk
cleanup_process $*
cleanup_data $db_name
cleanup_data_upstream $db_name

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
