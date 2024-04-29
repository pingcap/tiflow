#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$(cd $CUR/../_certificates && pwd)

export TICDC_USER=ticdc
export TICDC_PASSWORD=ticdc_secret
export TICDC_CA_PATH=$TLS_DIR/ca.pem
export TICDC_CERT_PATH=$TLS_DIR/client.pem
export TICDC_KEY_PATH=$TLS_DIR/client-key.pem

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
	# TODO: enable pulsar in the future.
	if [ "$SINK_TYPE" == "pulsar" ]; then
		exit 0
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true
	start_tls_tidb_cluster --workdir $WORK_DIR --tlsdir $TLS_DIR
	run_sql "CREATE USER 'ticdc'@'%' IDENTIFIED BY 'ticdc_secret';" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
		--ssl-ca=$TLS_DIR/ca.pem \
		--ssl-cert=$TLS_DIR/server.pem \
		--ssl-key=$TLS_DIR/server-key.pem

	cd $WORK_DIR
	pd_addr="https://$TLS_PD_HOST:$TLS_PD_PORT"

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${TLS_PD_HOST} ${TLS_PD_PORT} true)
	run_sql "CREATE table test.simple(id int primary key, val int);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
		--ssl-ca=$TLS_DIR/ca.pem \
		--ssl-cert=$TLS_DIR/server.pem \
		--ssl-key=$TLS_DIR/server-key.pem
	run_sql "CREATE table test.\`simple-dash\`(id int primary key, val int);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
		--ssl-ca=$TLS_DIR/ca.pem \
		--ssl-cert=$TLS_DIR/server.pem \
		--ssl-key=$TLS_DIR/server-key.pem

	cd $WORK_DIR
	echo " \
  [security]
   ca-path = \"$TLS_DIR/ca.pem\"
   cert-path = \"$TLS_DIR/server.pem\"
   key-path = \"$TLS_DIR/server-key.pem\"
   cert-allowed-cn = [\"fake_cn\"]
   client-user-required = true
   client-allowed-user = [\"ticdc\"]
  " >$WORK_DIR/server.toml
	run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--logsuffix "_${TEST_NAME}_tls1" \
		--pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
		--addr "127.0.0.1:8300" \
		--config "$WORK_DIR/server.toml" \
		--tlsdir "$TLS_DIR" \
		--cert-allowed-cn "client" # The common name of client.pem
	sleep 2

	TOPIC_NAME="ticdc-cli-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	uuid="custom-changefeed-name"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --tz="Asia/Shanghai" -c="$uuid"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer $WORK_DIR $SINK_URI ;;
	esac

	# Make sure changefeed is created.
	check_table_exists test.simple ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists test."\`simple-dash\`" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	check_changefeed_state "https://${TLS_PD_HOST}:${TLS_PD_PORT}" $uuid "normal" "null" "" $TLS_DIR

	check_changefeed_count https://${TLS_PD_HOST}:${TLS_PD_PORT} 1

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
case-sensitive = true
[scheduler]
enable-table-across-nodes = true
EOF
	set +e
	update_result=$(cdc cli changefeed update --pd=$pd_addr --config="$WORK_DIR/changefeed.toml" --no-confirm --changefeed-id $uuid)
	set -e
	if [[ ! $update_result == *"can only update changefeed config when it is stopped"* ]]; then
		echo "update changefeed config should fail when changefeed is running, got $update_result"
	fi

	# Pause changefeed
	run_cdc_cli changefeed --changefeed-id $uuid pause && sleep 3
	check_changefeed_state "https://${TLS_PD_HOST}:${TLS_PD_PORT}" $uuid "stopped" "null" "" $TLS_DIR

	# Update changefeed
	run_cdc_cli changefeed update --pd=$pd_addr --config="$WORK_DIR/changefeed.toml" --no-confirm --changefeed-id $uuid
	changefeed_info=$(curl -s -X GET "https://127.0.0.1:8300/api/v2/changefeeds/$uuid/meta_info" --cacert "${TLS_DIR}/ca.pem" --cert "${TLS_DIR}/client.pem" --key "${TLS_DIR}/client-key.pem" 2>&1)
	if [[ ! $changefeed_info == *"\"case_sensitive\":true"* ]]; then
		echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
		exit 1
	fi
	if [ "$SINK_TYPE" == "kafka" ]; then
		if [[ ! $changefeed_info == *"\"enable_table_across_nodes\":true"* ]]; then
			echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
			exit 1
		fi
	else
		# Currently, MySQL changefeed does not support scale out feature.
		if [[ $changefeed_info == *"\"enable_table_across_nodes\":true"* ]]; then
			echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
			exit 1
		fi
	fi

	# Resume changefeed
	run_cdc_cli changefeed --changefeed-id $uuid resume && sleep 3
	check_changefeed_state "https://${TLS_PD_HOST}:${TLS_PD_PORT}" $uuid "normal" "null" "" $TLS_DIR

	# Remove changefeed
	run_cdc_cli changefeed --changefeed-id $uuid remove && sleep 3
	check_changefeed_count https://${TLS_PD_HOST}:${TLS_PD_PORT} 0

	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --tz="Asia/Shanghai" -c="$uuid" && sleep 3
	check_changefeed_state "https://${TLS_PD_HOST}:${TLS_PD_PORT}" $uuid "normal" "null" "" $TLS_DIR

	# Make sure bad sink url fails at creating changefeed.
	badsink=$(run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="mysql://badsink" 2>&1 | grep -oE 'fail')
	if [[ -z $badsink ]]; then
		echo "[$(date)] <<<<< unexpect output got ${badsink} >>>>>"
		exit 1
	fi

	# Test Kafka SSL connection.
	if [ "$SINK_TYPE" == "kafka" ]; then
		SSL_TOPIC_NAME="ticdc-cli-test-ssl-$RANDOM"
		SINK_URI="kafka://127.0.0.1:9093/$SSL_TOPIC_NAME?protocol=open-protocol&ca=${TLS_DIR}/ca.pem&cert=${TLS_DIR}/client.pem&key=${TLS_DIR}/client-key.pem&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760&insecure-skip-verify=true"
		run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --tz="Asia/Shanghai"
	fi

	# Smoke test unsafe commands
	echo "y" | run_cdc_cli unsafe delete-service-gc-safepoint
	run_cdc_cli unsafe reset --no-confirm --pd=$pd_addr
	REGION_ID=$(pd-ctl --cacert="${TLS_DIR}/ca.pem" --cert="${TLS_DIR}/client.pem" --key="${TLS_DIR}/client-key.pem" -u=$pd_addr region | jq '.regions[0].id')
	TS=$(cdc cli tso query --pd=$pd_addr)
	# wait for owner online
	sleep 3
	run_cdc_cli unsafe resolve-lock --region=$REGION_ID
	run_cdc_cli unsafe resolve-lock --region=$REGION_ID --ts=$TS

	# Smoke test change log level
	curl -X POST -d '"warn"' https://127.0.0.1:8300/api/v1/log --cacert "${TLS_DIR}/ca.pem" --cert "${TLS_DIR}/client.pem" --key "${TLS_DIR}/client-key.pem"
	sleep 3
	# make sure TiCDC does not panic
	curl https://127.0.0.1:8300/status --cacert "${TLS_DIR}/ca.pem" --cert "${TLS_DIR}/client.pem" --key "${TLS_DIR}/client-key.pem"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
