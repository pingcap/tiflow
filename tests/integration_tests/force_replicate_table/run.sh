#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_data_subset() {
	tbl=$1
	up_host=$2
	up_port=$3
	down_host=$4
	down_port=$5
	for i in $(seq 0 100); do
		stmt="select * from $tbl order by id limit $i,1\G"
		query=$(mysql -h${up_host} -P${up_port} -uroot -e "${stmt}")
		clean_query="${query//\*/}"
		if [ -n "$clean_query" ]; then
			data_id=$(echo $clean_query | awk '{print $(NF-2)}')
			data_a=$(echo $clean_query | awk '{print $(NF)}')
			condition=""
			if [[ "$data_id" -eq "NULL" ]]; then
				condition="id is NULL"
			else
				condition="id=$data_id"
			fi
			condition="${condition} AND "
			if [[ "$data_a" -eq "NULL" ]]; then
				condition="${condition} a is NULL"
			else
				condition="${condition} a=$data_a"
			fi
			stmt2="select * from $tbl where $condition"
			query2=$(mysql -h${down_host} -P${down_port} -uroot -e "${stmt2}")
			if [ -z "$query2" ]; then
				echo "id=$data_id,a=$data_a doesn't exist in downstream table $tbl"
				exit 1
			fi
		fi
	done
}

export -f check_data_subset

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --tidb-config $CUR/conf/tidb_config.toml

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-force_replicate_table-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?safe-mode=true" ;;
	esac
	cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config $CUR/conf/changefeed.toml
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" $CUR/conf/changefeed.toml
	fi

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for i in $(seq 0 6); do
		table="force_replicate_table.t$i"
		check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	# data could be duplicated due to https://github.com/pingcap/tiflow/issues/964,
	# so we just check downstream contains all data in upstream.
	for i in $(seq 0 6); do
		ensure 10 check_data_subset "force_replicate_table.t$i" \
			${UP_TIDB_HOST} ${UP_TIDB_PORT} ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
