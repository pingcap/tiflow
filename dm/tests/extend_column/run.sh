#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
db="extend_column"
tb="t"
yb="y"

function run() {
	# table `y` has extend and generate column
	# table `t` has extend and different instance
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# create table in tidb
	run_sql_file $cur/data/tidb.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task in all mode
	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"

	# check load data
	# only extend table
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb};" "count(1): 4"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c_table='1';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c_table='2';" "count(1): 2"
	# cross mysql instance
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c_table='2' and c_schema='extend_column2' and c_source='replica02';" "count(1): 2"

	# check incremental data
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_sql_tidb_with_retry "select count(1) from ${db}.${yb};" "count(1): 6"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 6"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=3 and c_table='1';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=3 and c_table='2';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=3 and c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=3 and c_table='2' and c_schema='extend_column2' and c_source='replica02';" "count(1): 1"

	# check update data
	run_sql_file $cur/data/db1.update.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.update.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=3 and c_table='1';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=4 and c_table='1';" "count(1): 0"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=3 and c_table='2';" "count(1): 0"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=4 and c_table='2';" "count(1): 1"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=3 and c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=4 and c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 0"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=3 and c_table='2' and c_schema='extend_column2' and c_source='replica02';" "count(1): 0"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=4 and c_table='2' and c_schema='extend_column2' and c_source='replica02';" "count(1): 1"

	# check delete data
	run_sql_file $cur/data/db1.delete.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.delete.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_sql_tidb_with_retry "select count(1) from ${db}.${yb};" "count(1): 5"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=1 and c_table='1';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=2 and c_table='1';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=3 and c_table='1';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=4 and c_table='1';" "count(1): 0"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=1 and c_table='2';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=2 and c_table='2';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=3 and c_table='2';" "count(1): 0"
	run_sql_tidb_with_retry "select count(1) from ${db}.${yb} where c1=4 and c_table='2';" "count(1): 0"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=1 and c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=2 and c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=3 and c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=4 and c_table='1' and c_schema='extend_column1' and c_source='replica01';" "count(1): 0"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1=1 and c_table='2' and c_schema='extend_column2' and c_source='replica02';" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c1>1 and c_table='2' and c_schema='extend_column2' and c_source='replica02';" "count(1): 0"

}

cleanup_data extend_column1
cleanup_data extend_column2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
