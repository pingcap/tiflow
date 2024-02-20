#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=10

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="ticdc-changefeed-pause-resume-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar) SINK_URI="pulsar+ssl://127.0.0.1:6651/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	now=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	# 90s after now
	target_ts=$(($now + 90 * 10 ** 3 * 2 ** 18))

	changefeed_id="changefeed_finish"

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
	  cdc cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id} --config="$CUR/conf/pulsar_test.toml" --target-ts=$target_ts 2>&1 | tail -n2 | head -n1 | awk '{print $2}';;
	*) cdc cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id} --target-ts=$target_ts 2>&1 | tail -n2 | head -n1 | awk '{print $2}';;
	esac

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer $WORK_DIR $SINK_URI ;;
	esac

	run_sql "CREATE DATABASE changefeed_finish;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table changefeed_finish.t (id int primary key auto_increment, t datetime DEFAULT CURRENT_TIMESTAMP)"
	for i in $(seq 1 10); do
		run_sql "insert into changefeed_finish.t values (),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	sleep 90

	ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "finished" "null" ""

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
