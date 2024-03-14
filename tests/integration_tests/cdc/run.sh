#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	stop_tidb_cluster

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-cdc-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR oauth
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	if [ "$SINK_TYPE" == "pulsar" ]; then
		cat <<EOF >>$WORK_DIR/pulsar_test.toml
              [sink.pulsar-config.oauth2]
              oauth2-issuer-url="http://localhost:9096"
              oauth2-audience="cdc-api-uri"
              oauth2-client-id="1234"
              oauth2-private-key="${WORK_DIR}/credential.json"
EOF
	else
		echo "" >$WORK_DIR/pulsar_test.toml
	fi
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config $WORK_DIR/pulsar_test.toml
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --oauth2-private-key ${WORK_DIR}/credential.json --oauth2-issuer-url "http://localhost:9096" -- oauth2-client-id "1234" ;;
	esac
}

trap stop_tidb_cluster EXIT
# storage and pulsar is not supported yet.
# TODO(dongmen): enable pulsar in the future.
if [ "$SINK_TYPE" != "storage" ]; then
	# TODO(dongmen): enable pulsar in the future.
	if [ "$SINK_TYPE" == "pulsar" ]; then
		exit 0
	fi

	prepare $*
	cd "$(dirname "$0")"
	set -o pipefail
	GO111MODULE=on go run cdc.go -config ./config.toml 2>&1 | tee $WORK_DIR/tester.log
	cleanup_process $CDC_BINARY
	check_logs $WORK_DIR
fi
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
