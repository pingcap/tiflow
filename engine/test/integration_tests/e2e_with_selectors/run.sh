#!/bin/bash

set -eu

WORK_DIR=$OUT_DIR/$TEST_NAME
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml"
CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $CONFIG)
echo "using adjusted configs to deploy cluster: $CONFIG"

function run() {
	start_engine_cluster $CONFIG
	# add a delay in case that the cluster is not ready
	sleep 3

	create_job_json=$(base64 -i $CUR_DIR/conf/fake_job.json | tr -d \\n | jq -Rs '{ type: "FakeJob", selectors: [{ label: "name", target: "exec-1", op: "Eq" }], config: . }')
	echo "create_job_json: $create_job_json"
	job_id=$(curl -X POST -H "Content-Type: application/json" -d "$create_job_json" "http://127.0.0.1:10245/api/v1/jobs?tenant_id=e2e_selectors&project_id=e2e_selectors" | tee /dev/stderr | jq -r .id)
	echo "job_id: $job_id"

	exec_with_retry --count 100 "curl \"http://127.0.0.1:10245/api/v1/jobs/$job_id\" | tee /dev/stderr | jq -e '.state == \"Finished\"'"
}

trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
