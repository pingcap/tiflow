#!/bin/bash

set -eu

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK_DIR=$OUT_DIR/$TEST_NAME
CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml $DOCKER_COMPOSE_DIR/dm_databases.yaml"

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_engine_cluster $CONFIG
	wait_mysql_online.sh --host 127.0.0.1 --port 3306 --password 123456
	wait_mysql_online.sh --host 127.0.0.1 --port 3307 --password 123456
	wait_mysql_online.sh --host 127.0.0.1 --port 4000

	read -p 123
}

trap "stop_engine_cluster $CONFIG" EXIT
run $*
# TODO: handle log properly
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
