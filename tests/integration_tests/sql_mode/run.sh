#!/bin/bash

# [DISCRIPTION]
# This test is for when upstream server is set to non-default sql mode.
# We test NO_BACKSLASH_ESCAPES mode and ANSI_QUOTES here.
# We will test the following cases:
# 1. upstream server is set to NO_BACKSLASH_ESCAPES mode. downstream server is set to NO_BACKSLASH_ESCAPES mode.
# 2. upstream server is set to ANSI_QUOTES mode. downstream server is set to ANSI_QUOTES mode.
#
# for two cases, we will insert some dml / ddl influenced by sql mode, and check the result.
# case 1
# use test;
# create table t20(id bigint primary key, a text, b text as ((regexp_replace(a, "^[1-9]\d{9,29}$", "aaaaa"))), c text);
# insert into t20 (id, a, c) values(1,123456, 'ab\\\\c');
# insert into t20 (id, a, c) values(2,1234567890123, 'ab\\c');

# case 2
# use test;
#create table t1(id bigint primary key, a date);
#insert into t1 values(1, '2023-02-08');

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

# case 1
rm -rf $WORK_DIR && mkdir -p $WORK_DIR
start_tidb_cluster --workdir $WORK_DIR
trap stop_tidb_cluster EXIT

run_sql "set global sql_mode='NO_BACKSLASH_ESCAPES';" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
run_sql "set global sql_mode='NO_BACKSLASH_ESCAPES';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

cd $WORK_DIR
start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="test-1"

run_sql "use test; create table t1(id bigint primary key, a text, b text as ((regexp_replace(a, '^[1-9]\d{9,29}$', 'aaaaa'))), c text); insert into t1 (id, a, c) values(1,123456, 'ab\\\\\\\\c'); insert into t1 (id, a, c) values(2,1234567890123, 'ab\\\\c');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

if [ "$SINK_TYPE" == "mysql" ]; then
	check_table_exists "test.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	sleep 10
	run_sql "SELECT * from test.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "b: 123456" &&
		check_contains "b: aaaaa" &&
		check_contains "c: ab\\\\\\\\c" &&
		check_contains "c: ab\\\\c"
fi

stop_tidb_cluster
start_tidb_cluster --workdir $WORK_DIR

## case 2
run_sql "set global sql_mode='ANSI_QUOTES';" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
run_sql "set global sql_mode='ANSI_QUOTES';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="test-2"

run_sql "use test; create table t2(id bigint primary key, a date); insert into t2 values(1, '2023-02-08');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

if [ "$SINK_TYPE" == "mysql" ]; then
	check_table_exists "test.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	sleep 10
	run_sql "SELECT * from test.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "a: 2023-02-08"
fi

stop_tidb_cluster
