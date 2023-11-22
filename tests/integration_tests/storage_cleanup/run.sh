#!/bin/bash

set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

EXIST_FILES=()
CLEANED_FILES=()
function generate_single_table_files() {
	local workdir=$1
	local bucket=$2
	local schema=$3
	local table=$4
	local day=$5
	local file_cnt=$6
	local should_clean=$7 # true or false

	table_dir=$workdir/$bucket/$schema/$table/$day
	mkdir -p $table_dir
	for i in $(seq 1 $file_cnt); do
		file=$table_dir/$i.data
		touch $file
		if [ "$should_clean" == "true" ]; then
			CLEANED_FILES+=($file)
		else
			EXIST_FILES+=($file)
		fi
	done

	mkdir -p $table_dir/meta
	touch $table_dir/meta/CDC.index
}

function generate_historic_files() {
	local target_bucket="storage_test"
	yesterday=$(date -d "yesterday" +"%Y-%m-%d")             # should not be cleaned since file-expiration-days is 1
	day_before_yesterday=$(date -d "2 days ago" +"%Y-%m-%d") # should be cleaned

	# historic files of table in schema.sql
	generate_single_table_files $WORK_DIR $target_bucket test multi_data_type $yesterday 10 false
	generate_single_table_files $WORK_DIR $target_bucket test multi_charset $day_before_yesterday 10 true
	generate_single_table_files $WORK_DIR $target_bucket test binary_columns $day_before_yesterday 10 true

	# historic files of tables in test but not in schema.sql
	generate_single_table_files $WORK_DIR $target_bucket test multi_data_type_dummy $day_before_yesterday 10 true
	generate_single_table_files $WORK_DIR $target_bucket test multi_charset_dummy $day_before_yesterday 10 true
	generate_single_table_files $WORK_DIR $target_bucket test binary_columns_dummy $yesterday 10 false

	# historic files of table belongs to different schema
	generate_single_table_files $WORK_DIR $target_bucket test2 multi_data_type $day_before_yesterday 10 true
	generate_single_table_files $WORK_DIR $target_bucket test2 multi_charset $day_before_yesterday 10 true
	generate_single_table_files $WORK_DIR $target_bucket test2 binary_columns $yesterday 10 false

	# historic files in default bucket, which should not be cleaned
	generate_single_table_files $WORK_DIR storage_test_default test multi_data_type 2022-01-01 10 false
	generate_single_table_files $WORK_DIR storage_test_default test multi_charset 2022-01-02 10 false
	generate_single_table_files $WORK_DIR storage_test_default test binary_columns 2022-01-03 10 false
}

function check_file_exists() {
	local all_should_exist=$1
	for f in ${EXIST_FILES[@]}; do
		if [ ! -f $f ]; then
			echo "file $f should exist but not"
			exit 1
		fi
	done

	for f in ${CLEANED_FILES[@]}; do
		if [ "$all_should_exist" == "true" ]; then
			if [ ! -f $f ]; then
				echo "file $f should exist but not"
				exit 1
			fi
		else
			if [ -f $f ]; then
				echo "file $f should not exist but exists"
				exit 1
			fi
		fi
	done
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

	SINK_URI_DEFAULT="file://$WORK_DIR/storage_test_default?flush-interval=5s"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI_DEFAULT" -c "default-config-test" --config=$CUR/conf/changefeed-default.toml
	sleep 20
	check_file_exists true

	SINK_URI="file://$WORK_DIR/storage_test?flush-interval=5s"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml
	sleep 20
	check_file_exists false

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
