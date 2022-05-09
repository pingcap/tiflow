#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# check resolved ts has been persisted in redo log meta
function check_resolved_ts() {
	changefeedid=$1
	check_tso=$2
	redo_dir=$3
	rts=$(cdc redo meta --storage="nfs:///tmp/tidb_cdc_test/consistent_replicate_nfs/nfs/redo" --tmp-dir="$redo_dir" | grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}')
	if [[ "$rts" -gt "$check_tso" ]]; then
		return
	fi
	echo "global resolved ts $rts not forward to $check_tso"
	exit 1
}

export -f check_resolved_ts

function run() {
	# we only support eventually consistent replication with MySQL sink
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	changefeed_id=$(cdc cli changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	run_sql "CREATE DATABASE consistent_replicate_nfs;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=consistent_replicate_nfs
	run_sql "CREATE table consistent_replicate_nfs.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "consistent_replicate_nfs.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "consistent_replicate_nfs.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# Inject the failpoint to prevent sink execution, but the global resolved can be moved forward.
	# Then we can apply redo log to reach an eventual consistent state in downstream.
	cleanup_process $CDC_BINARY
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/mysql/MySQLSinkHangLongTime=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	run_sql "create table consistent_replicate_nfs.USERTABLE2 like consistent_replicate_nfs.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into consistent_replicate_nfs.USERTABLE2 select * from consistent_replicate_nfs.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# to ensure row changed events have been replicated to TiCDC
	sleep 5

	nfs_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	current_tso=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	ensure 20 check_resolved_ts $changefeed_id $current_tso $nfs_download_path
	cleanup_process $CDC_BINARY

	export GO_FAILPOINTS=''
	cdc redo apply --tmp-dir="$nfs_download_path" --storage="nfs://$WORK_DIR/nfs/redo" --sink-uri="mysql://normal:123456@127.0.0.1:3306/"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
