#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

help_cnt=45

function run() {
	# check dmctl output with help flag
	# it should usage for root command
	$PWD/bin/dmctl.test DEVEL --help >$WORK_DIR/help.log
	help_msg=$(cat $WORK_DIR/help.log)
	help_msg_cnt=$(echo "${help_msg}" | wc -l | xargs)
	if [ "$help_msg_cnt" != $help_cnt ]; then
		echo "dmctl case 1 help failed: $help_msg"
		exit 1
	fi

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	# check dmctl command start-task output with master-addr
	# it should usage for start-task
	$PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT start-task >$WORK_DIR/help.log 2>&1 && exit 1 || echo "exit code should be not zero"
	help_msg=$(cat $WORK_DIR/help.log)

	echo $help_msg | grep -q "dmctl start-task"
	if [ $? -ne 0 ]; then
		echo "dmctl case 2 help failed: $help_msg"
		exit 1
	fi

	# check dmctl command start-task output with master-addr and unknown flag
	# it should print unknown command xxxx
	$PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT xxxx start-task >$WORK_DIR/help.log 2>&1 && exit 1 || echo "exit code should be not zero"
	help_msg=$(cat $WORK_DIR/help.log)
	help_msg_cnt=$(echo "${help_msg}" | wc -l | xargs)
	if [ "$help_msg_cnt" -lt 1 ]; then
		echo "dmctl case 3 help failed: $help_msg"
		exit 1
	fi
	echo $help_msg | grep -q "unknown command \"xxxx\" for \"dmctl\""
	if [ $? -ne 0 ]; then
		echo "dmctl case 3 help failed: $help_msg"
		exit 1
	fi

	# check dmctl command start-task --help
	# it should print the usage for start-task
	$PWD/bin/dmctl.test DEVEL start-task --help >$WORK_DIR/help.log
	help_msg=$(cat $WORK_DIR/help.log)
	help_msg_cnt=$(echo "${help_msg}" | grep "Usage:" | wc -l)
	if [ "$help_msg_cnt" -ne 1 ]; then
		echo "dmctl case 4 help failed: $help_msg"
		exit 1
	fi

	# run normal task with command
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# check wrong backoff-max
	cp $cur/conf/source1.yaml $WORK_DIR/wrong-source.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/wrong-source.yaml
	sed -i "s/backoff-max: 5m/backoff-max: 0.1s/g" $WORK_DIR/wrong-source.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/wrong-source.yaml" \
		"\"result\": false" 1 \
		"Please increase \`backoff-max\`" 1

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source1.yaml -w $WORKER1_NAME" \
		"\"result\": true" 2 \
		"\"source\": \"mysql-replica-01\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --name worker1" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-01\"" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source2.yaml -w wrong-worker" \
		"\"result\": false" 1 \
		"not exists" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source2.yaml -w worker1" \
		"\"result\": false" 1 \
		"not free" 1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	source $cur/../dmctl_basic/check_list/check_task.sh
	# check wrong do-tables
	cp $cur/conf/dm-task.yaml $WORK_DIR/wrong-dm-task.yaml
	sed -i "/do-dbs:/a\    do-tables:\n    - db-name: \"dmctl_command\"" $WORK_DIR/wrong-dm-task.yaml
	sed -i "/do-dbs:/d" $WORK_DIR/wrong-dm-task.yaml
	echo "ignore-checking-items: [\"all\"]" >>$WORK_DIR/wrong-dm-task.yaml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/wrong-dm-task.yaml" \
		"Table string cannot be empty" 1

	# check wrong chunk-filesize
	cp $cur/conf/dm-task.yaml $WORK_DIR/wrong-dm-task.yaml
	sed -i "s/chunk-filesize: 64/chunk-filesize: 6qwe4/g" $WORK_DIR/wrong-dm-task.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/wrong-dm-task.yaml" \
		"invalid \`chunk-filesize\` 6qwe4" 1

	# start DM task with command mode
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml" \
		"\`remove-meta\` in task config is deprecated, please use \`start-task ... --remove-meta\` instead" 1
	check_log_contains $WORK_DIR/master/log/dm-master.log "\`remove-meta\` in task config is deprecated, please use \`start-task ... --remove-meta\` instead"

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 2\/1\/1\"" 1 \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 2

	# query status with command mode
	$PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT query-status >$WORK_DIR/query-status.log

	# use DM_MASTER_ADDR env
	export DM_MASTER_ADDR="127.0.0.1:$MASTER_PORT"
	run_dm_ctl_with_retry $WORK_DIR "" \
		"query-status test" \
		"worker1" 1

	running_task=$(grep -r Running $WORK_DIR/query-status.log | wc -l | xargs)

	if [ "$running_task" != 1 ]; then
		echo "query status failed with command: $running_task task"
		exit 1
	fi
}

function checktask_full_mode_conn() {
	# full mode
	# dumpers: (10 + 2) for each
	# loaders: (16 + 1) * 2 = 34
	run_sql_source1 "set @@GLOBAL.max_connections=11;"
	run_sql_source2 "set @@GLOBAL.max_connections=11;"
	check_task_not_pass $cur/conf/dm-task2.yaml # dumper threads too few
	run_sql_source1 "set @@GLOBAL.max_connections=12;"
	run_sql_source2 "set @@GLOBAL.max_connections=12;"
	check_task_pass $cur/conf/dm-task2.yaml

	run_sql "set @@GLOBAL.max_connections=33;" $TIDB_PORT $TIDB_PASSWORD # loader threads too few
	check_task_not_pass $cur/conf/dm-task2.yaml
	run_sql "set @@GLOBAL.max_connections=34;" $TIDB_PORT $TIDB_PASSWORD
	check_task_pass $cur/conf/dm-task2.yaml

	run_sql_source1 "set @@GLOBAL.max_connections=151;"
	run_sql_source2 "set @@GLOBAL.max_connections=151;"
}

function checktask_incr_mode_conn() {
	# all mode
	# syncers: (16 + 5) * 2 = 42
	run_sql "set @@GLOBAL.max_connections=41;" $TIDB_PORT $TIDB_PASSWORD
	check_task_not_pass $cur/conf/dm-task3.yaml # not enough connections for syncer
	run_sql "set @@GLOBAL.max_connections=42;" $TIDB_PORT $TIDB_PASSWORD
	check_task_not_pass $cur/conf/dm-task3.yaml # pass

	# set to default
	run_sql "set @@GLOBAL.max_connections=151;" $TIDB_PORT $TIDB_PASSWORD
}

function check_full_mode_conn() {
	run_sql_tidb "drop database if exists dmctl_conn"
	run_sql_both_source "drop database if exists dmctl_conn"
	run_sql_both_source "create database dmctl_conn"
	# ref: many_tables/run.sh
	for ((i = 0; i <= 500; ++i)); do
		run_sql_source1 "create table dmctl_conn.test_$i(id int primary key)"
		run_sql_source1 "insert into dmctl_conn.test_$i values (1)"
	done
	dmctl_start_task "$cur/conf/dm-task2.yaml" --remove-meta
	run_sql_source1 'SHOW PROCESSLIST;'
	check_rows_equal 13 # 12 + 1 for SHOWPROCESSLIST

	sleep 5
	run_sql_tidb 'SHOW PROCESSLIST;'
	check_rows_equal 35 # (16 + 1) * 2 + 1 for SHOW PROCESSLIST= 35

	dmctl_stop_task "test"
	run_sql_tidb "drop database if exists dm_meta" # cleanup checkpoint
	run_sql_tidb "drop database if exists dmctl_conn"
}

function check_incr_mode_conn() {
	run_sql_tidb "drop database if exists dmctl_conn"
	run_sql_both_source "drop database if exists dmctl_conn"
	run_sql_both_source "create database dmctl_conn"
	dmctl_start_task "$cur/conf/dm-task3.yaml" --remove-meta

	run_sql_tidb 'SHOW PROCESSLIST;'
	check_rows_equal 43 # (16 + 5) * 2 + 1 for show processlist
	dmctl_stop_task "test"
	run_sql_tidb "drop database if exists dm_meta" # cleanup checkpoint
	run_sql_tidb "drop database if exists dmctl_conn"
}

function run_validator_cmd {
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	# skip incremental rows with id <= 2
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(2)'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml" \
		"\`remove-meta\` in task config is deprecated, please use \`start-task ... --remove-meta\` instead" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	# do not do sync diff this time, will fail
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 2\/1\/1\"" 1 \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 2\/0\/0" 1 \
		"\"stage\": \"Running\"" 4 \
		"\"stage\": \"Stopped\"" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status --table-stage running test" \
		"\"stage\": \"Running\"" 4 \
		"\"stage\": \"Stopped\"" 0
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status --table-stage stopped test" \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 1 \
		"no primary key" 1

	dmctl_stop_task "test"
	echo "clean up data" # pre-check will not pass, since there is a table without pk
	cleanup_data_upstream dmctl_command
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/    mode: full/    mode: none/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml --remove-meta
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator not found for task" 2
}

function run_check_task() {
	source $cur/../dmctl_basic/check_list/check_task.sh
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/loader/longLoadProcess=return(true);github.com/pingcap/tiflow/dm/dumpling/longDumpProcess=return(true)'

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	check_incr_mode_conn
	check_full_mode_conn
	checktask_full_mode_conn
	checktask_incr_mode_conn
}

cleanup_data dmctl_command
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

# run validator commands
cleanup_data dmctl_command
run_validator_cmd $*
cleanup_process $*

# run check task
cleanup_data dmctl_command
run_check_task
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
