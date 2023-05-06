#!/bin/bash

set -exu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
TEST_HOST_LIST=(UP_TIDB_HOST UP_PD_HOST_{1..3} UP_TIKV_HOST_{1..3})
# FIXME: hostname in macOS doesn't support -I option.
lan_addrs=($(hostname -I))
lan_addr=${lan_addrs[0]-"127.0.0.1"}
export UP_TIDB_HOST=$lan_addr \
	UP_PD_HOST_1=$lan_addr \
	UP_PD_HOST_2=$lan_addr \
	UP_PD_HOST_3=$lan_addr \
	UP_TIKV_HOST_1=$lan_addr \
	UP_TIKV_HOST_2=$lan_addr \
	UP_TIKV_HOST_3=$lan_addr

proxy_pid=""
proxy_port=$(shuf -i 10081-20081 -n1)
function start_proxy() {
	echo "dumpling grpc packet to $WORK_DIR/packets.dump..."
	GO111MODULE=on WORK_DIR=$WORK_DIR go run $CUR/run-proxy.go --port=$proxy_port >$WORK_DIR/packets.dump &
	proxy_pid=$!
}

function stop_proxy() {
	kill "$proxy_pid" || true
}

function prepare() {
	for host in "${TEST_HOST_LIST[@]}"; do
		echo "$host $(printenv $host)"
		case $(printenv $host) in
		# Should we handle ::1/128 here?
		127.*.*.*)
			echo "[WARNING] http_proxies: the host of component $host is loopback, hence proxies would be ignored, skipping this test..."
			exit 0
			;;
		*) ;;
		esac
	done

	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"
	stop_tidb_cluster
	start_tidb_cluster --workdir $WORK_DIR

	start_proxy
	# TODO: make sure the proxy is started.
	sleep 5
	export http_proxy=http://127.0.0.1:$proxy_port
	export https_proxy=http://127.0.0.1:$proxy_port
	ensure 10 curl http://$UP_PD_HOST_1:2379/

	echo started proxy at $proxy_pid

	cd $WORK_DIR
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="blackhole:///"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
}

function check() {
	services=($(cat $WORK_DIR/packets.dump | xargs -L1 dirname | sort | uniq))
	service_type_count=${#services[@]}
	echo "captured services: "
	echo ${services[@]}
	# at least two types:
	#   "pdpb.PD"
	#   "tikvpb.TiKV"
	if ! [ $service_type_count -ge 2 ]; then
		echo "services didn't match expected."
		echo "[total count]: $service_type_count (expected >= 2)"
		exit 1
	fi
}

trap "stop_tidb_cluster && stop_proxy" EXIT

prepare
sleep 5
check

check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
