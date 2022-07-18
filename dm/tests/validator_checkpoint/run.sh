#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
source $cur/../_utils/shardddl_lib.sh
WORK_DIR=$TEST_DIR/$TEST_NAME

db_name=$TEST_NAME

function prepare_for_test() {
	cleanup_process $*
	cleanup_data $db_name
	cleanup_data_upstream $db_name

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2
}

function trigger_validator_flush() {
	sleep 0.2
	run_sql_source1 "alter table $db_name.t1_1 comment 'a';" # force flush checkpoint
	run_sql_source2 "alter table $db_name.t2_1 comment 'a';" # force flush checkpoint
}

function run() {
	#
	# persis success for 2 source, and data of different source didn't interrupt with each other
	#
	echo "--> check persist checkpoint and data with 2 source"
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(5)'
	prepare_for_test
	dmctl_start_task $cur/conf/dm-task.yaml --remove-meta
	# wait until task is in sync unit
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"\"unit\": \"Sync\"" 2
	run_sql_source1 "insert into $db_name.t1_1 values(1, 1)"
	run_sql_source1 "insert into $db_name.t1_1 values(2, 2)"
	run_sql_source1 "insert into $db_name.t1_1 values(6, 6)"
	run_sql_source1 "update $db_name.t1_1 set value=22 where id=2"
	run_sql_source1 "insert into $db_name.t1_2 values(6)"
	run_sql_source1 "insert into $db_name.t1_3 values(6)"
	run_sql_source2 "insert into $db_name.t2_1 values(1)"
	run_sql_source2 "insert into $db_name.t2_2 values(6)"
	trigger_validator_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"processedRowsStatus\": \"insert\/update\/delete: 3\/1\/0\"" 1 \
		"processedRowsStatus\": \"insert\/update\/delete: 1\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 1\/1\/0" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 1\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 2
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change where source='mysql-replica-01'" \
		"count(*): 2"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_validator_checkpoint a, dm_meta.test_validator_pending_change b
                           where a.source=b.source
                           		 and a.source='mysql-replica-01'
                               and a.revision=b.revision" \
		"count(*): 2"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change where source='mysql-replica-02'" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_validator_checkpoint a, dm_meta.test_validator_pending_change b
                           where a.source=b.source
                           		 and a.source='mysql-replica-02'
                               and a.revision=b.revision" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-01'" \
		"count(*): 3"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-02'" \
		"count(*): 2"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_error_change" \
		"count(*): 0"
	# validator don't support relay right now, so we don't check binlog_name
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_syncer_checkpoint a, dm_meta.test_validator_checkpoint b
                           where a.cp_schema=''
                               and a.id=b.source
                               and a.binlog_pos=b.binlog_pos
                               and a.binlog_gtid=b.binlog_gtid;" \
		"count(*): 2"

	echo "--> check validator can restart from previous position on fail over"
	restart_worker1
	restart_worker2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"processedRowsStatus\": \"insert\/update\/delete: 3\/1\/0\"" 1 \
		"processedRowsStatus\": \"insert\/update\/delete: 1\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 1\/1\/0" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 1\/0\/0" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 2
	# insert missed data manually
	run_sql_tidb "insert into $db_name.t1_1 values(1, 1)"
	run_sql_tidb "insert into $db_name.t1_1 values(2, 22)"
	run_sql_tidb "insert into $db_name.t2_1 values(1)"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"processedRowsStatus\": \"insert\/update\/delete: 3\/1\/0\"" 1 \
		"processedRowsStatus\": \"insert\/update\/delete: 1\/0\/0\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 2
	trigger_validator_flush
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change" \
		"count(*): 0"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-01'" \
		"count(*): 3"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-02'" \
		"count(*): 2"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_error_change" \
		"count(*): 0"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_syncer_checkpoint a, dm_meta.test_validator_checkpoint b
                           where a.cp_schema=''
                               and a.id=b.source
                               and a.binlog_pos=b.binlog_pos
                               and a.binlog_gtid=b.binlog_gtid;" \
		"count(*): 2"

	#
	# validator persist fail in the middle
	#
	echo "--> check validator persist fail in the middle"
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(10)'
	export GO_FAILPOINTS="$GO_FAILPOINTS;github.com/pingcap/tiflow/dm/syncer/ValidatorFailOnPersist=return()"
	prepare_for_test
	dmctl_start_task $cur/conf/dm-task.yaml --remove-meta
	# wait until task is in sync unit
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"\"unit\": \"Sync\"" 2
	for id in $(seq 4); do
		run_sql_source1 "insert into $db_name.t1_1 values($id, $id)"
		run_sql_source2 "insert into $db_name.t2_1 values($id)"
	done
	# trigger a success persist with 4 pending rows
	trigger_validator_flush
	run_sql_source1 "insert into $db_name.t1_1 values(5, 5)"
	run_sql_source2 "insert into $db_name.t2_1 values(5)"
	# this persist fails
	trigger_validator_flush
	# validator stopped because of error, stage of table is running
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"stage\": \"Stopped\"" 2 \
		"\"stage\": \"Running\"" 2 \
		"processedRowsStatus\": \"insert\/update\/delete: 5\/0\/0\"" 2 \
		"pendingRowsStatus\": \"insert\/update\/delete: 5\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 2
	#  processed ins/upd/del should be 4/0/0 in both source
	run_sql_tidb_with_retry "select concat_ws('/', procd_ins, procd_upd, procd_del) processed
													 from dm_meta.test_validator_checkpoint where source='mysql-replica-01'" \
		"processed: 4/0/0"
	run_sql_tidb_with_retry "select concat_ws('/', procd_ins, procd_upd, procd_del) processed
													 from dm_meta.test_validator_checkpoint where source='mysql-replica-02'" \
		"processed: 4/0/0"
	# 4 of valid persist, 2 of incomplete persist in source 01
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change where source='mysql-replica-01'" \
		"count(*): 6"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_validator_checkpoint a, dm_meta.test_validator_pending_change b
                           where a.source=b.source
                           		 and a.source='mysql-replica-01'
                               and a.revision=b.revision" \
		"count(*): 4"
	# 4 of valid persist, 3 of incomplete persist in source 02
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change where source='mysql-replica-02'" \
		"count(*): 7"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_validator_checkpoint a, dm_meta.test_validator_pending_change b
                           where a.source=b.source
                           		 and a.source='mysql-replica-02'
                               and a.revision=b.revision" \
		"count(*): 4"
	# 1 table for each source, 0 error
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-01'" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-02'" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_error_change" \
		"count(*): 0"
	# validator should falls behind syncer
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_syncer_checkpoint a, dm_meta.test_validator_checkpoint b
                           where a.cp_schema=''
                               and a.id=b.source
                               and a.binlog_pos!=b.binlog_pos" \
		"count(*): 2"

	echo "--> after resume and persist again, meta data should be consistent"
	dmctl_stop_task $cur/conf/dm-task.yaml
	dmctl_start_task $cur/conf/dm-task.yaml
	trigger_validator_flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"stage\": \"Running\"" 4 \
		"processedRowsStatus\": \"insert\/update\/delete: 5\/0\/0\"" 2 \
		"pendingRowsStatus\": \"insert\/update\/delete: 5\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 2
	#  processed ins/upd/del should be 5/0/0 in both source
	run_sql_tidb_with_retry "select concat_ws('/', procd_ins, procd_upd, procd_del) processed
													 from dm_meta.test_validator_checkpoint where source='mysql-replica-01'" \
		"processed: 5/0/0"
	run_sql_tidb_with_retry "select concat_ws('/', procd_ins, procd_upd, procd_del) processed
													 from dm_meta.test_validator_checkpoint where source='mysql-replica-02'" \
		"processed: 5/0/0"
	# 5 pending row for source 01
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change where source='mysql-replica-01'" \
		"count(*): 5"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_validator_checkpoint a, dm_meta.test_validator_pending_change b
                           where a.source=b.source
                           		 and a.source='mysql-replica-01'
                               and a.revision=b.revision" \
		"count(*): 5"
	# 5 pending row for source 02
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change where source='mysql-replica-02'" \
		"count(*): 5"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_validator_checkpoint a, dm_meta.test_validator_pending_change b
                           where a.source=b.source
                           		 and a.source='mysql-replica-02'
                               and a.revision=b.revision" \
		"count(*): 5"
	# 1 table for each source, 0 error
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-01'" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-02'" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_error_change" \
		"count(*): 0"
	# validator should reach syncer
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_syncer_checkpoint a, dm_meta.test_validator_checkpoint b
                           where a.cp_schema=''
                               and a.id=b.source
                               and a.binlog_pos=b.binlog_pos
                               and a.binlog_gtid=b.binlog_gtid;" \
		"count(*): 2"

	echo "--> check validate success after insert data manually"
	for id in $(seq 5); do
		run_sql_tidb "insert into $db_name.t1_1 values($id, $id)"
		run_sql_tidb "insert into $db_name.t2_1 values($id)"
	done
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"stage\": \"Running\"" 4 \
		"processedRowsStatus\": \"insert\/update\/delete: 5\/0\/0\"" 2 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 2
	trigger_validator_flush
	#  processed ins/upd/del should be 5/0/0 in both source
	run_sql_tidb_with_retry "select concat_ws('/', procd_ins, procd_upd, procd_del) processed
													 from dm_meta.test_validator_checkpoint where source='mysql-replica-01'" \
		"processed: 5/0/0"
	run_sql_tidb_with_retry "select concat_ws('/', procd_ins, procd_upd, procd_del) processed
													 from dm_meta.test_validator_checkpoint where source='mysql-replica-02'" \
		"processed: 5/0/0"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_pending_change" \
		"count(*): 0"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-01'" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_table_status where source='mysql-replica-02'" \
		"count(*): 1"
	run_sql_tidb_with_retry "select count(*) from dm_meta.test_validator_error_change" \
		"count(*): 0"
	run_sql_tidb_with_retry "select count(*)
                           from dm_meta.test_syncer_checkpoint a, dm_meta.test_validator_checkpoint b
                           where a.cp_schema=''
                               and a.id=b.source
                               and a.binlog_pos=b.binlog_pos
                               and a.binlog_gtid=b.binlog_gtid;" \
		"count(*): 2"
}

run $*
cleanup_process $*
cleanup_data $db_name
cleanup_data_upstream $db_name

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
