#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
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
    i=$(($i+1))
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

function run() {
    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
    run_sql "CREATE DATABASE consistent_replicate_s3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=consistent_replicate_s3

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-sink-retry-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&max-message-bytes=102400&kafka-version=${KAFKA_VERSION}";;
        *) SINK_URI="mysql://normal:123456@127.0.0.1:3306/";;
    esac
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}"
    fi

    run_sql "CREATE table consistent_replicate_s3.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "consistent_replicate_s3.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists "consistent_replicate_s3.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "truncate table consistent_replicate_s3.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    run_sql "CREATE table consistent_replicate_s3.check2(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "consistent_replicate_s3.check2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=consistent_replicate_s3
    run_sql "CREATE table consistent_replicate_s3.check3(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "consistent_replicate_s3.check3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "create table consistent_replicate_s3.USERTABLE2 like consistent_replicate_s3.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "insert into consistent_replicate_s3.USERTABLE2 select * from consistent_replicate_s3.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table consistent_replicate_s3.check4(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "consistent_replicate_s3.USERTABLE2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists "consistent_replicate_s3.check4" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
