#!/bin/bash

set -eu

OUT_DIR=/tmp/tidb_cdc_test
CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export PATH=$PATH:$CUR/_utils:$CUR/../bin

mkdir -p $OUT_DIR || true

if [ "${1-}" = '--debug' ]; then
    WORK_DIR=$OUT_DIR/debug
    trap stop_tidb_cluster EXIT

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    PATH="$CUR/../bin:$CUR/_utils:$PATH" \
    LD_LIBRARY_PATH="$CUR/../bin:$CUR/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME="debug" \
    start_tidb_cluster --workdir $WORK_DIR

    cdc server --log-file $WORK_DIR/cdc.log --log-level debug --addr 127.0.0.1:8300 > $WORK_DIR/stdout.log 2>&1 &
    sleep 1
    cdc cli changefeed create --sink-uri="mysql://root@127.0.0.1:3306/"

    echo 'You may now debug from another terminal. Press [ENTER] to exit.'
    read line
    exit 0
fi

run_case() {
    local case=$1
    local script=$2
    local sink_type=$3
    echo "Running test $script using Sink-Type: $sink_type..."
    PATH="$CUR/../bin:$CUR/_utils:$PATH" \
    LD_LIBRARY_PATH="$CUR/../bin:$CUR/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME=$case \
    bash "$script" "$sink_type"
}

sink_type=$1

set +eu
test_case=$2

if [ -z "$test_case" ]; then
    test_case="*"
fi
set -eu

if [ "$test_case" == "*" ]; then
    for script in $CUR/*/run.sh; do
        test_name="$(basename "$(dirname "$script")")"
        run_case $test_name $script $sink_type
    done
else
    for name in $test_case; do
        script="$CUR/$name/run.sh"
        run_case $name $script $sink_type
    done
fi

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
