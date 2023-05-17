#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_ts_not_forward() {
	changefeed_id=$1
	ts1=$(cdc cli changefeed query -c "$changefeed_id" | jq -r '.checkpoint_tso')
	sleep 1
	ts2=$(cdc cli changefeed query -c "$changefeed_id" | jq -r '.checkpoint_tso')
	if [ "$ts1" == "$ts2" ]; then
		count=0
		while [ "$ts1" == "$ts2" ]; do
			sleep 1
			ts2=$(cdc cli changefeed query -c "$changefeed_id" | jq -r '.checkpoint_tso')
			((count++))
			if [ $count -ge 10 ]; then
				echo "pass check, checkpoint tso not forward after 10s"
				return
			fi
		done
	fi
	exit 1
}

function check_ts_forward() {
	changefeedid=$1
	rts1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.resolved_ts')
	checkpoint1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.checkpoint_tso')
	sleep 1
	rts2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.resolved_ts')
	checkpoint2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.checkpoint_tso')
	if [[ "$rts1" != "null" ]] && [[ "$rts1" != "0" ]]; then
		if [[ "$rts1" -ne "$rts2" ]] || [[ "$checkpoint1" -ne "$checkpoint2" ]]; then
			echo "changefeed is working normally rts: ${rts1}->${rts2} checkpoint: ${checkpoint1}->${checkpoint2}"
			return
		fi
	fi
	exit 1
}

export -f check_ts_not_forward
export -f check_ts_forward

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	# this test contains `recover table`, which requires super privilege, so we
	# can't use the normal user
	TOPIC_NAME="ticdc-common-1-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://root@127.0.0.1:3306/" ;;
	esac
	changefeed_id="ddl-only-block-related-table"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id}

	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_sql_file $CUR/data/start.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists ddl_only_block_related_table.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	kill_cdc_pid $owner_pid
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/ExecuteNotDone=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	run_sql_file $CUR/data/finishe.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# make sure t1,t2 are equal in upstream and downstream
	# we should not check this if sink type is kafka, since the checkpoint is not advance
	# so the kafka consumer will not consume the dmls of t1,t2 behind the stuck DDL's commitTs
	# the next check diff in line 69 will check the eventual consistency of all tables
	# and it is enough to ensure the correctness of the test
	if [ "$SINK_TYPE" == "mysql" ]; then
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 90
	fi
	# check checkpoint does not advance
	ensure 30 check_ts_not_forward $changefeed_id

	kill_cdc_pid $owner_pid
	# clear failpoint, so the `ddl_not_done` table can advance
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	# make sure all tables are equal in upstream and downstream
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_2.toml 90

	# check checkpoint advance
	ensure 20 check_ts_forward $changefeed_id

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
