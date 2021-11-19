#!/bin/bash

set -eu

CLUSTER_NAME="dm-v2"

DM_VER=$2

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PATH=$CUR/../_utils:$PATH # for sync_diff_inspector

source $CUR/lib.sh

function deploy_dm() {
	# install TiUP-DM
	curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
	source /root/.profile
	tiup install dm

	tiup install dmctl:$DM_VER

	tiup dm deploy --yes $CLUSTER_NAME $DM_VER $CUR/conf/topo.yaml
	tiup dm start --yes $CLUSTER_NAME
}

function migrate_before_upgrade() {
	exec_full_stage

	tiup dmctl:$DM_VER --master-addr=master1:8261 operate-source create $CUR/conf/source1.yaml
	tiup dmctl:$DM_VER --master-addr=master1:8261 operate-source create $CUR/conf/source2.yaml

	tiup dmctl:$DM_VER --master-addr=master1:8261 start-task $CUR/conf/task.yaml

	exec_incremental_stage1

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

function migrate_after_upgrade() {
	exec_incremental_stage2

	sleep 10

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	tiup dmctl:$DM_VER --master-addr=master1:8261 stop-task $TASK_NAME
}

function destroy_v2_by_tiup() {
	tiup dm destroy --yes $CLUSTER_NAME
}

# run this before upgrade TiDB.
function before_upgrade() {
	install_sync_diff

	deploy_dm

	migrate_before_upgrade
}

# run this after upgrade TiDB.
function after_upgrade() {
	migrate_after_upgrade

	# destroy_v2_by_tiup
}

$1
