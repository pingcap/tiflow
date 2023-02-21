#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This test mainly verifies CDC can handle the following scenario
# 1. Two captures, capture-1 is the owner, each capture replicates more than one table.
# 2. capture-2 replicates some DMLs but has some delay, such as large amount of
#    incremental scan data, sink block, etc, we name this slow table as table-slow.
# 3. Before capture-2 the checkpoint ts of table-slow reaches global resolved ts,
#    a rebalance operation is triggered, either by manual rebalance or a new capture
#    joins the cluster. So a delete table operation will be dispatched to capture-2,
#    and the boundary ts is global resolved ts. capture-2 will continue to replicate
#    table-slow until the checkpoint ts reaches the boundary ts.
# 4. However, before the checkpoint ts of table-slow reaches boundary ts, capture-2
#    suicides itself because of some network issue or PD jitter.
# 5. After the cluster recovers, the data of table-slow in downstream should be
#    consistent with upstream.
#
# In this test, step-2 is achieved by failpoint injection, step-3 is triggered
# by manual rebalance, step-4 is achieved by revoking the lease of capture key.
function run() {
	# test with mysql sink only
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 1 --addr "127.0.0.1:8300"
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/dmlsink/txn/mysql/MySQLSinkHangLongTime=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 2 --addr "127.0.0.1:8301"

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	run_sql "CREATE DATABASE capture_suicide_while_balance_table;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for i in $(seq 1 4); do
		run_sql "CREATE table capture_suicide_while_balance_table.t$i (id int primary key auto_increment)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	for i in $(seq 1 4); do
		check_table_exists "capture_suicide_while_balance_table.t$i" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done

	capture1_id=$(cdc cli capture list | jq -r '.[]|select(.address=="127.0.0.1:8300")|.id')
	capture2_id=$(cdc cli capture list | jq -r '.[]|select(.address=="127.0.0.1:8301")|.id')

	target_capture=$capture1_id
	one_table_id=$(cdc cli processor query -c $changefeed_id -p $capture2_id | jq -r '.status.tables|keys[0]')
	if [[ $one_table_id == "null" ]]; then
		target_capture=$capture2_id
		one_table_id=$(cdc cli processor query -c $changefeed_id -p $capture1_id | jq -r '.status.tables|keys[0]')
	fi
	table_query=$(mysql -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -uroot -e "select table_name from information_schema.tables where tidb_table_id = ${one_table_id}\G")
	table_name=$(echo $table_query | tail -n 1 | awk '{print $(NF)}')
	run_sql "insert into capture_suicide_while_balance_table.${table_name} values (),(),(),(),()"

	# sleep some time to wait global resolved ts forwarded
	sleep 2
	curl -X POST http://127.0.0.1:8300/capture/owner/move_table -d "cf-id=${changefeed_id}&target-cp-id=${target_capture}&table-id=${one_table_id}"
	# sleep some time to wait table balance job is written to etcd
	sleep 2

	# revoke lease of etcd capture key to simulate etcd session done
	lease=$(ETCDCTL_API=3 etcdctl get /tidb/cdc/default/__cdc_meta__/capture/${capture2_id} -w json | grep -o 'lease":[0-9]*' | awk -F: '{print $2}')
	lease_hex=$(printf '%x\n' $lease)
	ETCDCTL_API=3 etcdctl lease revoke $lease_hex

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
