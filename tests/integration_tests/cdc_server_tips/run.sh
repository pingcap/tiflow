#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE="mysql"
stdout_file=$WORK_DIR/stdout.log
cdc_launched=

function try_to_run_cdc() {
	# $*: invalid args to avoid lauching cdc server
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql "CREATE table test.simple1(id int primary key, val int);"

	if [[ $1 == "valid" ]]; then
		echo "try a VALID cdc server command"
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	else
		echo "try an INVALID cdc server command"
		run_cdc_server --supposed-to-fail "true" --workdir $WORK_DIR --binary $CDC_BINARY --pd "None"
	fi

	#Wait the failed cdc to quit
	sleep 20

	if [[ $(ps -a | grep "cdc.test") == "" ]]; then
		cdc_launched="false"
		echo 'Failed to start cdc, the usage tips should be printed'
		return 0
	else
		cdc_launched="true"
		echo 'Succeed to run cdc, now create a changefeed, no usage tips should be printed'
		echo "pid"$(ps -a | grep "cdc.test")
	fi

	TOPIC_NAME="ticdc-server-tips-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka+ssl://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-client-id=cdc_server_tips&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql+ssl://normal:123456@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	echo 'Succeed to create a changefeed, no usage tips should be printed'
}

function region_label_test() {
	i=0
	while [ -z "$(curl -X GET http://127.0.0.1:2379/pd/api/v1/config/region-label/rules 2>/dev/null | grep 'meta')" ]; do
		i=$((i + 1))
		if [ "$i" -gt 5 ]; then
			echo 'Failed to verify meta region labels'
			exit 1
		fi
		sleep 1
	done
	echo 'succeed to verify meta placement rules'
}

stop_cdc_and_do_check() {
	if [[ "$cdc_launched" == "true" ]]; then
		region_label_test
		echo "Later, cdc will receive a signal(SIGINT) and exit"
		sleep 60
		cdc_pid=$(ps -a | grep -m 1 "cdc.test" | awk '{print $1}')
		echo "cdc pid is "$cdc_pid
		kill -2 $cdc_pid
	fi
	sleep 10
	check_usage_tips $stdout_file $cdc_launched
}

# invalid command and should print usage tips
trap stop_tidb_cluster EXIT
try_to_run_cdc "invalid"
stop_cdc_and_do_check
echo " 1st test case $TEST_NAME success! "

# should not print usage tips
trap stop_tidb_cluster EXIT
try_to_run_cdc "valid"
stop_cdc_and_do_check
echo " 2nd test case $TEST_NAME success! "

echo "[$(date)] <<<<<< run all test cases $TEST_NAME success! >>>>>> "
