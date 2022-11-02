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
	stop_minio
	stop_tidb_cluster
}

s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$S3_ENDPOINT --host-bucket=$S3_ENDPOINT --no-ssl mb s3://logbucket

function run() {
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
   	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    SINK_URI="s3://logbucket/storage_test?flush-interval=5s&endpoint=http://127.0.0.1:24927/"
    run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

    run_sql_file $CUR/data/schema.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql_file $CUR/data/schema.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_storage_consumer $WORK_DIR  "s3://logbucket/storage_test?endpoint=http://127.0.0.1:24927/" $CUR/conf/changefeed.toml
    sleep 8
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
