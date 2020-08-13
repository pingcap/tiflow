#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test


function prepare() {
    rm -rf "$WORK_DIR"
    mkdir -p "$WORK_DIR"
    stop_tidb_cluster
    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    SINK_URI="local://$WORK_DIR/test?"

    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
}

function cdclog_test() {
  run_sql "drop database if exists $TEST_NAME"
  run_sql "create database $TEST_NAME"
  run_sql "create table $TEST_NAME.t1 (c0 int primary key, payload varchar(1024));"
  run_sql "create table $TEST_NAME.t2 (c0 int primary key, payload varchar(1024));"

  # wait for ddl synced
  sleep 8

  run_sql "insert into $TEST_NAME.t1 values (1, 'a')"
  run_sql "insert into $TEST_NAME.t1 values (2, 'b')"

  # wait for log synced
  sleep 8

  DATA_DIR="$WORK_DIR/test"
  # retrieve table id by log meta
  table_id=$(cat $DATA_DIR/log.meta | jq | grep t1 | awk -F '"' '{print $2}')
  file_count=$(ls -ahl $DATA_DIR/t_$table_id | grep cdclog | wc -l)
  if [[ ! "$file_count" -eq "1" ]]; then
      echo "$TEST_NAME failed, expect 1 row changed files, obtain $file_count"
      exit 1
  fi
  ddl_file_count=$(ls -ahl $DATA_DIR/ddls | grep ddl | wc -l)
  if [[ ! "$ddl_file_count" -eq "1" ]]; then
      echo "$TEST_NAME failed, expect 1 ddl file, obtain $ddl_file_count"
      exit 1
  fi
  # TODO test rotate file
  cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
prepare $*
cdclog_test $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
