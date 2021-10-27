#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function checksum() {
	read -d '' sql <<EOF
SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', uid, name, info, age, id_gen,
    CONCAT(ISNULL(uid), ISNULL(name), ISNULL(info), ISNULL(age), ISNULL(id_gen)))) AS UNSIGNED)) AS checksum
    FROM db_target.t_target WHERE (uid > 70000);
EOF
	run_sql "$sql" $TIDB_PORT $TIDB_PASSWORD
	echo $(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt")
}

function run() {
	run_sql "SET @@GLOBAL.SQL_MODE='NO_ZERO_IN_DATE,NO_ZERO_DATE'" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "SET @@GLOBAL.SQL_MODE='ANSI_QUOTES'" $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml

	# now, for pessimistic shard DDL, if interrupted after executed DDL but before flush checkpoint,
	# re-sync this DDL will cause the source try to sync the DDL of the previous lock again,
	# this will need to recover the replication manually,
	# so we do not interrupt the replication after executed DDL for this test case.
	#
	# now, for pessimistic shard DDL, owner and non-owner will reach a stage often not at the same time,
	# in order to simply the check and resume flow, only enable the failpoint for one DM-worker.
	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/FlushCheckpointStage=return(2)"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	export GO_FAILPOINTS=''

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task only
	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Sync" 2

	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"test\",worker=\"worker1\"}" 3 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"test\",worker=\"worker2\"}" 3 1 3

	# TODO: check sharding partition id
	# use sync_diff_inspector to check full dump loader
	echo "check sync diff for full dump and load"
	run_sql "SET @@GLOBAL.SQL_MODE=''" $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# the task should paused by `FlushCheckpointStage` failpoint before flush old checkpoint.
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before flush old checkpoint" 1
	# worker1 will failed and worker2 will still running.
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"test\",worker=\"worker1\"}" 3 2 4
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"test\",worker=\"worker2\"}" 3 1 3

	# resume-task to next stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before track DDL" 1

	# resume-task to next stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"failpoint error for FlushCheckpointStage before execute DDL" 1

	# resume-task to next stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	# TODO: check sharding partition id
	# use sync_diff_inspector to check data now!
	echo "check sync diff for the first increment replication"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# test create database, create table in sharding mode
	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
	printf "\n[[table-config.source-tables]]\ninstance-id = \"source-1\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >>$WORK_DIR/diff_config.toml
	printf "\n[[table-config.source-tables]]\ninstance-id = \"source-2\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >>$WORK_DIR/diff_config.toml
	echo "check sync diff for the second increment replication"
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml

	old_checksum=$(checksum)

	# test drop table, drop database, truncate table in sharding mode
	run_sql_file $cur/data/db1.increment3.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment3.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
	printf "\n[[table-config.source-tables]]\ninstance-id = \"source-1\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >>$WORK_DIR/diff_config.toml
	sed -i "s/^# range-placeholder/range = \"uid < 70000\"/g" $WORK_DIR/diff_config.toml
	echo "check sync diff for the third increment replication"
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml

	new_checksum=$(checksum)
	echo "checksum before drop/truncate: $old_checksum, checksum after drop/truncate: $new_checksum"
	[ "$old_checksum" == "$new_checksum" ]

	# test conflict ddl in single worker
	run_sql "alter table sharding1.t1 add column new_col1 int;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "alter table sharding1.t2 add column new_col2 int;" $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"detect inconsistent DDL sequence" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	# still conflict
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"detect inconsistent DDL sequence" 1

	# stop twice, just used to test stop by the way
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"task test has no source or not exist" 1

	check_metric_not_contains $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"test\",worker=\"worker1\"}" 3
	check_metric_not_contains $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"test\",worker=\"worker2\"}" 3

	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
}

cleanup_data db_target
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
