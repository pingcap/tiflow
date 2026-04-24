#!/bin/bash
# Cluster lifecycle operations for DM integration tests.
#
# Sourced by test_prepare. Provides functions to start, stop, and restart
# downstream TiDB clusters in both classic and next-gen modes.
#
# Startup delegates to standalone scripts (which manage their own processes).
# Cleanup runs in the test's shell (needs access to pgrep/kill).

CUR_CLUSTER_LIB=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

# Kill the port-4000 TiDB (SYSTEM TiDB on next-gen, unistore on classic).
cleanup_tidb_server() {
	local pattern='tidb-server.*-P 4000'
	local pids
	pids=$(pgrep -f "$pattern" || true)
	echo "tidb-server on port 4000 pids=${pids:-none}"
	if [ -n "$pids" ]; then
		kill -HUP $pids 2>/dev/null || true
	fi
	for _ in $(seq 1 120); do
		if ! pgrep -f "$pattern" >/dev/null 2>&1; then
			echo "tidb-server on port 4000 already exit"
			rm -f /tmp/*_tidb/*/tmp-storage/_dir.lock 2>/dev/null || true
			return 0
		fi
		sleep 1
	done
	echo "tidb-server on port 4000 didn't exit in 120s"
	pgrep -af "$pattern" || true
	return 1
}

# Tear down the full downstream cluster.
# Next-gen: only TiDB (preserve PD + TiKV + MinIO + tikv-worker).
# Classic: kill everything + clean unistore data.
cleanup_downstream_cluster() {
	if [ "${NEXT_GEN:-}" = "1" ]; then
		cleanup_tidb_server
	else
		killall -9 tidb-server 2>/dev/null || true
		killall -9 tikv-server 2>/dev/null || true
		killall -9 pd-server 2>/dev/null || true
		wait_process_exit tidb-server
		wait_process_exit tikv-server
		wait_process_exit pd-server
		rm -rf /tmp/tidb
	fi
}

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

# Start or restart a single downstream TiDB.
# Args: port password [config_file]
run_tidb_server() {
	"$CUR_CLUSTER_LIB/run_tidb_server" "$@"
}

# Start a full downstream cluster (PD + TiKV + TiDB).
# Classic: single PD + TiKV + TiDB.
# Next-gen: MinIO + PD + TiKV + tikv-worker + SYSTEM TiDB + user TiDB.
# Args: work_dir
run_downstream_cluster() {
	if [ "${NEXT_GEN:-}" = "1" ]; then
		"$CUR_CLUSTER_LIB/run_downstream_cluster_nextgen" "$@"
	else
		"$CUR_CLUSTER_LIB/run_downstream_cluster_classic" "$@"
	fi
}

# Start a TLS-enabled downstream cluster.
# Classic: full PD + TiKV + TiDB with TLS on separate ports.
# Next-gen: restart only user TiDB with client-facing TLS.
# Args: work_dir conf_dir cluster_ca cluster_cert cluster_key db_ca db_cert db_key
run_downstream_cluster_with_tls() {
	if [ "${NEXT_GEN:-}" = "1" ]; then
		"$CUR_CLUSTER_LIB/run_downstream_cluster_with_tls_nextgen" "$@"
	else
		"$CUR_CLUSTER_LIB/run_downstream_cluster_with_tls_classic" "$@"
	fi
}
