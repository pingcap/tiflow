#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
db1="downstream_diff_index1"
tb1="t1"
db2="downstream_diff_index2"
tb2="t2"
db="downstream_diff_index"
tb="t"

function run() {
	# create table in mysql with pk
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	# create table in tidb with different pk
	run_sql_file $cur/data/tidb.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

	# worker will inject delete/update sql check
	inject_points=(
		"github.com/pingcap/tiflow/pkg/sqlmodel/DownstreamTrackerWhereCheck=return()"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $cur/conf/source2.yaml $SOURCE_ID2

	# start DM task
	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
	# check full load data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1<100;" "count(1): 6"

	# downstream create diff uk
	run_sql_tidb "alter table ${db}.${tb} add unique key(c2);"

	# db1 increment data with update and delete
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# check update data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=1 and c3='111';" "count(1): 1"
	check_log_contain_with_retry '\[UpdateWhereColumnsCheck\] \[Columns="\[c2\]"\]' $WORK_DIR/worker1/log/dm-worker.log
	# check delete data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=2;" "count(1): 1"
	check_log_contain_with_retry '\[DeleteWhereColumnsCheck\] \[Columns="\[c2\]"\]' $WORK_DIR/worker1/log/dm-worker.log

	# alter schema to test pk
	run_sql_tidb "alter table ${db}.${tb} add primary key(c3);"
	run_sql_source1 "alter table ${db1}.${tb1} drop column c2;"
	run_sql_source2 "alter table ${db2}.${tb2} drop column c2;"

	# db2 increment data with update and delete
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	# check update data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=3 and c3='333';" "count(1): 1"
	check_log_contain_with_retry '\[UpdateWhereColumnsCheck\] \[Columns="\[c3\]"\]' $WORK_DIR/worker2/log/dm-worker.log
	# check delete data
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=1;" "count(1): 1"
	check_log_contain_with_retry '\[DeleteWhereColumnsCheck\] \[Columns="\[c3\]"\]' $WORK_DIR/worker2/log/dm-worker.log

	run_sql_tidb_with_retry "select count(1) from ${db}.varchar20000 where c=1;" "count(1): 1"
}

cleanup_data downstream_diff_index
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*
export GO_FAILPOINTS=''

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
