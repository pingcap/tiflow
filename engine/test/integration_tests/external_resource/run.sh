#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/1minio.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"

function run() {
	start_engine_cluster $CONFIG
	echo -e "\nstart to run external storage integration tests...\n"

	export ENGINE_S3_ACCESS_KEY=engine
	export ENGINE_S3_SECRET_KEY=engineSecret
	export ENGINE_S3_ENDPOINT="http://127.0.0.1:9000/"

	CGO_ENABLED=0 go test -timeout 300s -cover -count 1 -v -run ^TestIntegrationS3 github.com/pingcap/tiflow/engine/pkg/externalresource/internal/bucket

	CGO_ENABLED=0 go test -timeout 600s -cover -count 1 -v -run ^TestIntegrationBroker github.com/pingcap/tiflow/engine/pkg/externalresource/broker

	echo -e "\ncheck external storage integration tests result..."
	if cat $OUT_DIR/engine_it.log | grep "SKIP:"; then
		echo -e "some tests are skipped, please check env and engine_it.log\n"
		exit 1
	fi
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
