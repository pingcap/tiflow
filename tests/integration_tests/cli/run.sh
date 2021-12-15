#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$(cd $CUR/../_certificates && pwd)

function check_changefeed_state() {
	changefeedid=$1
	expected=$2
	output=$(cdc cli changefeed query --simple --changefeed-id $changefeedid --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1 2>&1)
	state=$(echo $output | grep -oE "\"state\": \"[a-z]+\"" | tr -d '" ' | awk -F':' '{print $(NF)}')
	if [ "$state" != "$expected" ]; then
		echo "unexpected state $output, expected $expected"
		exit 1
	fi
}

function check_changefeed_count() {
	pd_addr=$1
	expected=$2
	feed_count=$(cdc cli changefeed list --pd=$pd_addr | jq '.|length')
	if [[ "$feed_count" != "$expected" ]]; then
		echo "[$(date)] <<<<< unexpect changefeed count! expect ${expected} got ${feed_count} >>>>>"
		exit 1
	fi
	echo "changefeed count ${feed_count} check pass, pd_addr: $pd_addr"
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true

	cd $WORK_DIR
	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE table test.simple(id int primary key, val int);"
	run_sql "CREATE table test.\`simple-dash\`(id int primary key, val int);"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-cli-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	uuid="custom-changefeed-name"
	run_cdc_cli changefeed create --start-ts=$start_ts --sort-engine=memory --sink-uri="$SINK_URI" --tz="Asia/Shanghai" -c="$uuid"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_cdc_cli changefeed cyclic create-marktables \
		--cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/"

	# Make sure changefeed is created.
	check_table_exists tidb_cdc.repl_mark_test_simple ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists tidb_cdc."\`repl_mark_test_simple-dash\`" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	check_changefeed_state $uuid "normal"

	check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1} 1
	check_changefeed_count http://${UP_PD_HOST_2}:${UP_PD_PORT_2} 1
	check_changefeed_count http://${UP_PD_HOST_3}:${UP_PD_PORT_3} 1
	check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1},http://${UP_PD_HOST_2}:${UP_PD_PORT_2},http://${UP_PD_HOST_3}:${UP_PD_PORT_3} 1

	# Make sure changefeed can not be created if the name is already exists.
	set +e
	exists=$(run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="$uuid" 2>&1 | grep -oE 'already exists')
	set -e
	if [[ -z $exists ]]; then
		echo "[$(date)] <<<<< unexpect output got ${exists} >>>>>"
		exit 1
	fi

	# Update changefeed failed because changefeed is running
	cat - >"$WORK_DIR/changefeed.toml" <<EOF
case-sensitive = false
[mounter]
worker-num = 4
EOF
	set +e
	update_result=$(cdc cli changefeed update --pd=$pd_addr --config="$WORK_DIR/changefeed.toml" --no-confirm --changefeed-id $uuid)
	set -e
	if [[ ! $update_result == *"can only update changefeed config when it is stopped"* ]]; then
		echo "update changefeed config should fail when changefeed is running, got $update_result"
	fi

	# Pause changefeed
	run_cdc_cli changefeed --changefeed-id $uuid pause && sleep 3
	jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
	if [[ $jobtype != 1 ]]; then
		echo "[$(date)] <<<<< unexpect admin job type! expect 1 got ${jobtype} >>>>>"
		exit 1
	fi
	check_changefeed_state $uuid "stopped"

	# Update changefeed
	run_cdc_cli changefeed update --pd=$pd_addr --config="$WORK_DIR/changefeed.toml" --no-confirm --changefeed-id $uuid
	changefeed_info=$(run_cdc_cli changefeed query --changefeed-id $uuid 2>&1)
	if [[ ! $changefeed_info == *"\"case-sensitive\": false"* ]]; then
		echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
		exit 1
	fi
	if [[ ! $changefeed_info == *"\"worker-num\": 4"* ]]; then
		echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
		exit 1
	fi
	if [[ ! $changefeed_info == *"\"sort-engine\": \"memory\""* ]]; then
		echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
		exit 1
	fi

	jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
	if [[ $jobtype != 1 ]]; then
		echo "[$(date)] <<<<< unexpect admin job type! expect 1 got ${jobtype} >>>>>"
		exit 1
	fi

	# Resume changefeed
	run_cdc_cli changefeed --changefeed-id $uuid resume && sleep 3
	jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
	if [[ $jobtype != 0 ]]; then
		echo "[$(date)] <<<<< unexpect admin job type! expect 0 got ${jobtype} >>>>>"
		exit 1
	fi
	check_changefeed_state $uuid "normal"

	# Remove changefeed
	run_cdc_cli changefeed --changefeed-id $uuid remove && sleep 3
	check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1} 0

	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --tz="Asia/Shanghai" -c="$uuid" && sleep 3
	check_changefeed_state $uuid "normal"

	# Make sure bad sink url fails at creating changefeed.
	badsink=$(run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="mysql://badsink" 2>&1 | grep -oE 'fail')
	if [[ -z $badsink ]]; then
		echo "[$(date)] <<<<< unexpect output got ${badsink} >>>>>"
		exit 1
	fi

	# Test Kafka SSL connection.
	if [ "$SINK_TYPE" == "kafka" ]; then
		SSL_TOPIC_NAME="ticdc-cli-test-ssl-$RANDOM"
		SINK_URI="kafka://127.0.0.1:9093/$SSL_TOPIC_NAME?ca=${TLS_DIR}/ca.pem&cert=${TLS_DIR}/client.pem&key=${TLS_DIR}/client-key.pem&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --tz="Asia/Shanghai"
	fi

	# Smoke test unsafe commands
	echo "y" | run_cdc_cli unsafe delete-service-gc-safepoint
	run_cdc_cli unsafe reset --no-confirm

	# Smoke test change log level
	curl -X POST -d '"warn"' http://127.0.0.1:8300/admin/log
	sleep 3
	# make sure TiCDC does not panic
	curl http://127.0.0.1:8300/status

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
