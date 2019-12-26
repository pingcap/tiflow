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
    OUT_DIR=$OUT_DIR \
    TEST_NAME="debug" \
    start_tidb_cluster $WORK_DIR

    cdc server --log-file $WORK_DIR/cdc.log --log-level debug > $WORK_DIR/cdc.log 2>&1 &
    sleep 1
    cdc cli

    echo 'You may now debug from another terminal. Press [ENTER] to exit.'
    read line
    exit 0
fi

run_case() {
    local case=$1
    local script=$2
    echo "Running test $script..."
    PATH="$CUR/../bin:$CUR/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME=$case \
    bash "$script"
}

if [ "$#" -ge 1 ]; then
    test_case=$1
else
    test_case="*"
fi

if [ "$test_case" == "*" ]; then
    for script in $CUR/*/run.sh; do
        test_name="$(basename "$(dirname "$script")")"
        run_case $test_name $script
    done
else
    for name in $test_case; do
        script="$CUR/$name/run.sh"
        run_case $name $script
    done
fi

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
