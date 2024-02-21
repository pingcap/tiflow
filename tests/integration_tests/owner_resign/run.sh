#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# TODO: kafka sink has bug with this case, remove this after bug is fixed
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
	
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
	ensure 10 "cdc cli capture list --server http://127.0.0.1:8301 |jq '.|length'|grep -E '^1$'"

	TOPIC_NAME="ticdc-owner-resign-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://kafka01:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar) SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	changefeed_id="owner-resign"

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
	  run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id} --config="$CUR/conf/pulsar_test.toml" --server="127.0.0.1:8301" --start-ts=$start_ts;;
	*) run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id} --server="127.0.0.1:8301" --start-ts=$start_ts;;
	esac
	
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer $WORK_DIR $SINK_URI ;;
	esac

	run_sql "CREATE database owner_resign;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table owner_resign.t1(id int not null primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# wait table t1 is processed by cdc server
	ensure 10 "cdc cli processor list --server http://127.0.0.1:8301 |jq '.|length'|grep -E '^1$'"
	# check the t1 is replicated to downstream to make sure the t1 is dispatched to cdc1
	check_table_exists "owner_resign.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql "INSERT INTO owner_resign.t1 (id, val) values (1, 1);"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/ChangefeedOwnerDontUpdateCheckpoint=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8302" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
	ensure 10 "cdc cli capture list --server http://127.0.0.1:8302 |jq '.|length'|grep -E '^2$'"

	curl -X POST http://127.0.0.1:8301/api/v1/owner/resign
	sleep 3

	run_sql "TRUNCATE TABLE owner_resign.t1;"
	run_sql "INSERT INTO owner_resign.t1 (id, val) values (2, 2);"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	curl -X POST http://127.0.0.1:8302/api/v1/owner/resign
	sleep 10
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
