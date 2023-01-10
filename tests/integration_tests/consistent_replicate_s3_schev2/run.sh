#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# start the s3 server
export MINIO_ACCESS_KEY=cdcs3accesskey
export MINIO_SECRET_KEY=cdcs3secretkey
export MINIO_BROWSER=off
export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
export S3_ENDPOINT=127.0.0.1:24927
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
pkill -9 minio || true
bin/minio server --address $S3_ENDPOINT "$WORK_DIR/s3" &
MINIO_PID=$!
i=0
while ! curl -o /dev/null -v -s "http://$S3_ENDPOINT/"; do
	i=$(($i + 1))
	if [ $i -gt 30 ]; then
		echo 'Failed to start minio'
		exit 1
	fi
	sleep 2
done

stop_minio() {
	kill -2 $MINIO_PID
}

stop() {
	# to distinguish whether the test failed in the DML synchronization phase or the DDL synchronization phase
	echo $(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -e "select count(*) from consistent_replicate_s3.USERTABLE;")
	stop_minio
	stop_tidb_cluster
}

s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$S3_ENDPOINT --host-bucket=$S3_ENDPOINT --no-ssl mb s3://logbucket

function run() {
	# we only support eventually consistent replication with MySQL sink
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	changefeed_id=$(cdc cli changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	run_sql "CREATE DATABASE consistent_replicate_s3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=consistent_replicate_s3
	run_sql "CREATE table consistent_replicate_s3.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "consistent_replicate_s3.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "consistent_replicate_s3.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# Inject the failpoint to prevent sink execution, but the global resolved can be moved forward.
	# Then we can apply redo log to reach an eventual consistent state in downstream.
	cleanup_process $CDC_BINARY
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/mysql/MySQLSinkHangLongTime=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	run_sql "create table consistent_replicate_s3.USERTABLE2 like consistent_replicate_s3.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into consistent_replicate_s3.USERTABLE2 select * from consistent_replicate_s3.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# to ensure row changed events have been replicated to TiCDC
	sleep 10

	storage_path="s3://logbucket/test-changefeed?endpoint=http://127.0.0.1:24927/"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	current_tso=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	ensure 50 check_redo_resolved_ts $changefeed_id $current_tso $storage_path $tmp_download_path/meta
	cleanup_process $CDC_BINARY

	export GO_FAILPOINTS=''
	export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
	export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
	cdc redo apply --tmp-dir="$tmp_download_path/apply" \
		--storage="$storage_path" \
		--sink-uri="mysql://normal:123456@127.0.0.1:3306/"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
