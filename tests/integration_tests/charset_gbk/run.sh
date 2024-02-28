#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-charset-gbk-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR mtls
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	if [ "$SINK_TYPE" == "pulsar" ]; then
		cat <<EOF >>$WORK_DIR/pulsar_test.toml
      [sink.pulsar-config]
      tls-trust-certs-file-path="${workdir}/ca.cert.pem"
      auth-tls-private-key-path="${workdir}/broker_client.key-pk8.pem"
      auth-tls-certificate-path="${workdir}/broker_client.cert.pem"
EOF
		cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config=$WORK_DIR/pulsar_test.toml
	else
		cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	fi

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --ca "${WORK_DIR}/ca.cert.pem" --auth-tls-private-key-path "${WORK_DIR}/broker_client.key-pk8.pem" --auth-tls-certificate-path="${WORK_DIR}/broker_client.cert.pem" ;;
	esac
	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first

	check_table_exists charset_gbk_test0.t0 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists charset_gbk_test0.t1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists charset_gbk_test1.t0 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	echo "check table exists success"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
