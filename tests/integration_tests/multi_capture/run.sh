#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	# create $DB_COUNT databases and import initial workload
	for i in $(seq $DB_COUNT); do
		db="multi_capture_$i"
		run_sql "CREATE DATABASE $db;"
		go-ycsb load mysql -P $CUR/conf/workload1 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
	done

	# start $CDC_COUNT cdc servers, and create a changefeed
	for i in $(seq $CDC_COUNT); do
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "$i" --addr "127.0.0.1:830${i}"
	done

	TOPIC_NAME="ticdc-multi-capture-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar) SINK_URI="pulsar+ssl://127.0.0.1:6651/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	changefeed_id="multi-capture"
	case $SINK_TYPE in
	pulsar) 
	  ca_path=$(cd $CUR/../_certificates/pulsar_certs/ca && pwd)
	  ca_path="${ca_path}/ca.cert.pem"
	  client_token_path=$(cd $CUR/../_certificates/pulsar_certs && pwd)
	  client_token_path="${client_token_path}/client_credentials.json"
	  touch $CUR/conf/pulsar_test.toml
	  cat <<EOF >> $CUR/conf/pulsar_test.toml
[sink.pulsar-config]
tls-trust-certs-file-path="${ca_path}"
oauth2.oauth2-private-key="${client_token_path}"
oauth2.oauth2-issuer-url="https://dev-ys3tcsktsrfqui44.us.auth0.com"
oauth2.oauth2-audience="pulsar"
oauth2.oauth2-client-id="h2IA1jjyTkVAGKOxlxq5o91BFZBgpX6z"
EOF
	  run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id} --server="127.0.0.1:8301" --config="$CUR/conf/pulsar_test.toml" --start-ts=$start_ts;;
	*) run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id} --server="127.0.0.1:8301" --start-ts=$start_ts;;
	esac

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer $WORK_DIR $SINK_URI ;;
	esac

	# check tables are created and data is synchronized
	for i in $(seq $DB_COUNT); do
		check_table_exists "multi_capture_$i.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# add more data in upstream and check again
	for i in $(seq $DB_COUNT); do
		db="multi_capture_$i"
		go-ycsb load mysql -P $CUR/conf/workload2 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
