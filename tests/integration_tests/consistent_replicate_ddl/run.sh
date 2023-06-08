#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

stop() {
	# to distinguish whether the test failed in the DML synchronization phase or the DDL synchronization phase
	echo $(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -e "SELECT count(*) FROM consistent_replicate_ddl.usertable;")
	stop_tidb_cluster
}

function run() {
	# we only support eventually consistent replication with MySQL sink
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	run_sql "set @@global.tidb_enable_exchange_partition=on" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix consistent_replicate_ddl.server1

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	changefeed_id=$(cdc cli changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	run_sql "CREATE DATABASE consistent_replicate_ddl CHARACTER SET utf8 COLLATE utf8_unicode_ci" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=consistent_replicate_ddl
	run_sql "CREATE TABLE consistent_replicate_ddl.usertable1 like consistent_replicate_ddl.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE consistent_replicate_ddl.usertable2 like consistent_replicate_ddl.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE consistent_replicate_ddl.usertable3 like consistent_replicate_ddl.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE consistent_replicate_ddl.usertable_bak like consistent_replicate_ddl.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "consistent_replicate_ddl.usertable1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "consistent_replicate_ddl.usertable2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_table_exists "consistent_replicate_ddl.usertable3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_table_exists "consistent_replicate_ddl.usertable_bak" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	sleep 5
	cleanup_process $CDC_BINARY
	# Inject the failpoint to prevent sink execution, but the global resolved can be moved forward.
	# Then we can apply redo log to reach an eventual consistent state in downstream.
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/dmlsink/txn/mysql/MySQLSinkHangLongTime=return(true);github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql/MySQLSinkExecDDLDelay=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix consistent_replicate_ddl.server2

	# case 1:
	# global ddl tests -> ActionRenameTable
	# table ddl tests -> ActionDropTable
	run_sql "DROP TABLE consistent_replicate_ddl.usertable1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "RENAME TABLE consistent_replicate_ddl.usertable_bak TO consistent_replicate_ddl.usertable1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO consistent_replicate_ddl.usertable1 SELECT * FROM consistent_replicate_ddl.usertable limit 10" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "RENAME TABLE consistent_replicate_ddl.usertable1 TO consistent_replicate_ddl.usertable1_1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT IGNORE INTO consistent_replicate_ddl.usertable1_1 SELECT * FROM consistent_replicate_ddl.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# case 2:
	# global ddl tests -> ActionCreateSchema, ActionModifySchemaCharsetAndCollate
	# table ddl tests -> ActionMultiSchemaChange, ActionAddColumn, ActionDropColumn, ActionModifyTableCharsetAndCollate
	run_sql "CREATE DATABASE consistent_replicate_ddl1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER DATABASE consistent_replicate_ddl CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_replicate_ddl.usertable2 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO consistent_replicate_ddl.usertable2 SELECT * FROM consistent_replicate_ddl.usertable limit 20" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_replicate_ddl.usertable2 DROP COLUMN FIELD0" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_replicate_ddl.usertable2 ADD COLUMN dummy varchar(30)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# case 3:
	# global ddl tests -> ActionDropSchema, ActionRenameTables
	# table ddl tests -> ActionModifyColumn
	run_sql "DROP DATABASE consistent_replicate_ddl1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_replicate_ddl.usertable3 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "RENAME TABLE consistent_replicate_ddl.usertable2 to consistent_replicate_ddl.usertable2_1, consistent_replicate_ddl.usertable3 TO consistent_replicate_ddl.usertable3_1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_replicate_ddl.usertable3_1 MODIFY COLUMN FIELD1 varchar(100)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO consistent_replicate_ddl.usertable3_1 SELECT * FROM consistent_replicate_ddl.usertable limit 31" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "CREATE table consistent_replicate_ddl.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# to ensure row changed events have been replicated to TiCDC
	sleep 120
	cleanup_process $CDC_BINARY

	storage_path="file://$WORK_DIR/redo"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	export GO_FAILPOINTS=''

	rts=$(cdc redo meta --storage="$storage_path" --tmp-dir="$tmp_download_path" | grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}')
	sed "s/<placeholder>/$rts/g" $CUR/conf/diff_config.toml >$WORK_DIR/diff_config.toml

	cat $WORK_DIR/diff_config.toml
	cdc redo apply --tmp-dir="$tmp_download_path/apply" \
		--storage="$storage_path" \
		--sink-uri="mysql://normal:123456@127.0.0.1:3306/"

	check_table_exists "consistent_replicate_ddl.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
