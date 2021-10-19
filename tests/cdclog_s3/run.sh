#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

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
	if [ $i -gt 7 ]; then
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

function prepare() {
	stop_tidb_cluster
	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

}

success=0
function check_cdclog() {
	DATA_DIR="$WORK_DIR/s3/logbucket/test"
	# retrieve table id by log meta
	if [ ! -f $DATA_DIR/log.meta ]; then
		return
	fi
	table_id=$(cat $DATA_DIR/log.meta | jq | grep t1 | awk -F '"' '{print $2}')
	if [ ! -d $DATA_DIR/t_$table_id ]; then
		return
	fi
	file_count=$(ls -ahl $DATA_DIR/t_$table_id | grep cdclog | wc -l)
	if [[ ! "$file_count" -eq "2" ]]; then
		echo "$TEST_NAME failed, expect 2 row changed files, obtain $file_count"
		return
	fi
	if [ ! -d $DATA_DIR/ddls ]; then
		return
	fi
	ddl_file_count=$(ls -ahl $DATA_DIR/ddls | grep ddl | wc -l)
	if [[ ! "$ddl_file_count" -eq "1" ]]; then
		echo "$TEST_NAME failed, expect 1 ddl file, obtain $ddl_file_count"
		return
	fi
	success=1
}

function cdclog_test() {
	run_sql "drop database if exists $TEST_NAME"
	run_sql "create database $TEST_NAME"
	run_sql "create table $TEST_NAME.t1 (c0 int primary key, payload varchar(1024));"

	SINK_URI="s3://logbucket/test?endpoint=http://$S3_ENDPOINT/"

	run_cdc_cli changefeed create --start-ts=0 --sink-uri="$SINK_URI"

	run_sql "create table $TEST_NAME.t2 (c0 int primary key, payload varchar(1024));"

	run_sql "insert into $TEST_NAME.t1 values (1, 'a')"
	# because flush row changed events interval is 5 second
	# so sleep 20 second will generate two files
	sleep 20
	run_sql "insert into $TEST_NAME.t1 values (2, 'b')"

	i=0
	while [ $i -lt 30 ]; do
		check_cdclog
		if [ "$success" == 1 ]; then
			echo "check log successfully"
			break
		fi
		i=$(($i + 1))
		echo "check log failed $i-th time, retry later"
		sleep 2
	done
	cleanup_process $CDC_BINARY
}

trap stop EXIT
prepare $*
cdclog_test $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
