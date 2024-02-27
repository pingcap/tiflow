#!/bin/bash

# [DISCRIPTION]
# This test is for when upstream server is set to non-default sql mode.
# We use NO_BACKSLASH_ESCAPES mode as an example.
# We will test the following cases:
# 1. upstream server is set to NO_BACKSLASH_ESCAPES mode. downstream server is set to NO_BACKSLASH_ESCAPES mode.
# 2. upstream server is set to default mode. downstream server is set to default mode.
#
# for each case, we will insert some dml / ddl influenced by sql mode, and check the result.
# use test;
# create table t20(id bigint primary key, a text, b text as ((regexp_replace(a, "^[1-9]\d{9,29}$", "aaaaa"))), c text);
# insert into t20 (id, a, c) values(1,123456, 'ab\\\\c');
# insert into t20 (id, a, c) values(2,1234567890123, 'ab\\c');

# When the downstream is mysql or tidb, we need to check the result in downstream server.
# Otherwise, we just need to check it works without error.


set -xeu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4


rm -rf $WORK_DIR && mkdir -p $WORK_DIR
start_tidb_cluster --workdir $WORK_DIR

run_sql "set global sql_mode='NO_BACKSLASH_ESCAPES';" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
run_sql "set global sql_mode='NO_BACKSLASH_ESCAPES';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

trap stop_tidb_cluster EXIT

cd $WORK_DIR
start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="test-1"

# test for case 1: upstream server is set to NO_BACKSLASH_ESCAPES mode. downstream server is set to NO_BACKSLASH_ESCAPES mode.
run_sql "use test; create table t1(id bigint primary key, a text, b text as ((regexp_replace(a, '^[1-9]\d{9,29}$', 'aaaaa'))), c text); insert into t1 (id, a, c) values(1,123456, 'ab\\\\\\\\c'); insert into t1 (id, a, c) values(2,1234567890123, 'ab\\\\c');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

check_table_exists "test.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

sleep 100

run_sql "SELECT * from test.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "b: 123456" &&
		check_contains "b: aaaaa" &&
		check_contains "c: ab\\\\\\\\c" &&
		check_contains "c: ab\\\\c" 


