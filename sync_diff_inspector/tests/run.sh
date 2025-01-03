#!/bin/sh

set -eu

OUT_DIR=/tmp/tidb_tools_test

# assign default value to mysql config
if [[ -z ${MYSQL_HOST+x} ]]; then
    echo "set MYSQL_HOST as default value \"127.0.0.1\""
    export MYSQL_HOST="127.0.0.1"
fi
if [[ -z ${MYSQL_PORT+x} ]]; then
    echo "set MYSQL_PORT as default value 3306"
    export MYSQL_PORT=3306
fi

mkdir -p $OUT_DIR || true
# to the dir of this script
cd "$(dirname "$0")"

pwd=$(pwd)

export PATH=$PATH:$pwd/_utils
export PATH=$PATH:$(dirname $pwd)/bin

rm -rf $OUT_DIR || true

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true
}

start_services() {
    stop_services

    echo "Starting PD..."
    pd-server \
        --client-urls http://127.0.0.1:2379 \
        --log-file "$OUT_DIR/pd.log" \
        --data-dir "$OUT_DIR/pd" &
    # wait until PD is online...
    while ! curl -o /dev/null -sf http://127.0.0.1:2379/pd/api/v1/version; do
        sleep 1
    done

    # Tries to limit the max number of open files under the system limit
    cat - > "$OUT_DIR/tikv-config.toml" <<EOF
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
[raftstore]
# true (default value) for high reliability, this can prevent data loss when power failure.
sync-log = false
EOF

    echo "Starting TiKV..."
    tikv-server \
        --pd 127.0.0.1:2379 \
        -A 127.0.0.1:20160 \
        --log-file "$OUT_DIR/tikv.log" \
        -C "$OUT_DIR/tikv-config.toml" \
        -s "$OUT_DIR/tikv" &
    sleep 2

    # support tls connection
    cat - > "$OUT_DIR/tidb-config.toml" <<EOF
[security]
ssl-ca = "$pwd/conf/root.crt"
ssl-cert = "$pwd/conf/tidb.crt"
ssl-key = "$pwd/conf/tidb.key"
EOF

    echo "Starting TiDB..."
    tidb-server \
        -P 4000 \
        --store tikv \
        --path 127.0.0.1:2379 \
        --config "$OUT_DIR/tidb-config.toml" \
        --log-file "$OUT_DIR/tidb.log" &

    echo "Verifying TiDB is started..."
    check_db_status "127.0.0.1" 4000 "tidb" "$OUT_DIR/tidb.log"

    echo "Starting Upstream TiDB..."
    tidb-server \
        -P 4001 \
        --path=$OUT_DIR/tidb \
        --status=20080 \
        --log-file "$OUT_DIR/down_tidb.log" \
        -socket "$OUT_DIR/down_tidb.sock" &

    echo "Verifying Upstream TiDB is started..."
    check_db_status "127.0.0.1" 4001 "tidb" "$OUT_DIR/down_tidb.log"
}

trap stop_services EXIT
start_services

# set to the case name you want to run only for debug
do_case=""

for script in ./*/run.sh; do
    test_name="$(basename "$(dirname "$script")")"
    if [[ $do_case != "" && $test_name != $do_case ]]; then
        continue
    fi
    echo "*******************************************"
    echo "Running test $script..."
    echo "*******************************************"
    PATH="$pwd/../bin:$pwd/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME=$test_name \
    sh "$script"
done

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
