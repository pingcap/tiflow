#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function generate_single_table_files() {
	local workdir=$1
	local bucket=$2
	local schema=$3
	local table=$4
	local day=$5
	local file_cnt=$6

	table_dir=$workdir/$bucket/$schema/$table/$day
	mkdir -p $table_dir
	for i in $(seq 1 $file_cnt); do
		touch $table_dir/$i.data
	done

	mkdir -p $table_dir/meta
	touch $table_dir/meta/CDC.index
}

function generate_historic_files() {
	local target_bucket="storage_test"
	# historic files of table in schema.sql
	generate_single_table_files $WORK_DIR $target_bucket test multi_data_type 2022-01-01 10
	generate_single_table_files $WORK_DIR $target_bucket test multi_charset 2022-01-02 10
	generate_single_table_files $WORK_DIR $target_bucket test binary_columns 2022-01-03 10

	# historic files of tables in test but not in schema.sql
	generate_single_table_files $WORK_DIR $target_bucket test multi_data_type_dummy 2022-01-01 10
	generate_single_table_files $WORK_DIR $target_bucket test multi_charset_dummy 2022-01-02 10
	generate_single_table_files $WORK_DIR $target_bucket test binary_columns_dummy 2022-01-03 10

	# historic files of table belongs to different schema
	generate_single_table_files $WORK_DIR $target_bucket test2 multi_data_type 2022-01-01 10
	generate_single_table_files $WORK_DIR $target_bucket test2 multi_charset 2022-01-02 10
	generate_single_table_files $WORK_DIR $target_bucket test2 binary_columns 2022-01-03 10

	# historic files in different bucket, which should not be cleaned
	generate_single_table_files $WORK_DIR storage_test2 test multi_data_type 2022-01-01 10
	generate_single_table_files $WORK_DIR storage_test2 test multi_charset 2022-01-02 10
	generate_single_table_files $WORK_DIR storage_test2 test binary_columns 2022-01-03 10
}

function run() {
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	generate_historic_files

	# wait input
	read -p "Press enter to continue"

	SINK_URI="file://$WORK_DIR/storage_test?flush-interval=5s"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	run_sql_file $CUR/data/schema.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_storage_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml ""
	sleep 8
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
