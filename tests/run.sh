#!/bin/sh

set -eu

OUT_DIR=/tmp/tidb_cdc_test

mkdir -p $OUT_DIR || true
# to the dir of this script
cd "$(dirname "$0")"

pwd=$(pwd)

export PATH=$PATH:$pwd/_utils
export PATH=$PATH:$(dirname $pwd)/bin


clean_data() {
    rm -rf $OUT_DIR/* || true
}

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true

    killall -9 cdc || true
}

start_services() {
    stop_services
    clean_data

    echo "Starting PD..."
    pd-server \
        --client-urls http://127.0.0.1:2379 \
        --log-file "$OUT_DIR/pd.log" \
        --data-dir "$OUT_DIR/pd" &

    echo "Starting downstream PD..."
    pd-server \
        --client-urls http://127.0.0.1:2381 \
        --peer-urls http://127.0.0.1:2382 \
        --log-file "$OUT_DIR/down_pd.log" \
        --data-dir "$OUT_DIR/down_pd" &

    # wait until PD is online...
    while ! curl -o /dev/null -sf http://127.0.0.1:2379/pd/api/v1/version; do
        sleep 1
    done

    # wait until downstream PD is online...
    while ! curl -o /dev/null -sf http://127.0.0.1:2381/pd/api/v1/version; do
        sleep 1
    done

# TODO set it split region normally.
    # Tries to limit the max number of open files under the system limit
    cat - > "$OUT_DIR/tikv-config.toml" <<EOF
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
[raftstore]
# true (default value) for high reliability, this can prevent data loss when power failure.
sync-log = false

# make tikv don't split now
[coprocessor]
## When it is set to `true`, TiKV will try to split a Region with table prefix if that Region
## crosses tables.
## It is recommended to turn off this option if there will be a large number of tables created.
split-region-on-table = false

## One split check produces several split keys in batch. This config limits the number of produced
## split keys in one batch.
# batch-split-limit = 10

## When Region [a,e) size exceeds `region_max_size`, it will be split into several Regions [a,b),
## [b,c), [c,d), [d,e) and the size of [a,b), [b,c), [c,d) will be `region_split_size` (or a
## little larger).
region-max-size = "100000MB"
region-split-size = "100000MB"

## When the number of keys in Region [a,e) exceeds the `region_max_keys`, it will be split into
## several Regions [a,b), [b,c), [c,d), [d,e) and the number of keys in [a,b), [b,c), [c,d) will be
## `region_split_keys`.
region-max-keys = 100000000
region-split-keys = 100000000
EOF

# tidb server config file
    cat - > "$OUT_DIR/tidb-config.toml" <<EOF
# When create table, split a separated region for it. It is recommended to
# turn off this option if there will be a large number of tables created.
split-table = false
EOF

    echo "Starting TiKV..."
    tikv-server \
        --pd 127.0.0.1:2379 \
        -A 127.0.0.1:20160 \
        --log-file "$OUT_DIR/tikv.log" \
        -C "$OUT_DIR/tikv-config.toml" \
        -s "$OUT_DIR/tikv" &

    echo "Starting downstream TiKV..."
    tikv-server \
        --pd 127.0.0.1:2381 \
        -A 127.0.0.1:20161 \
        --log-file "$OUT_DIR/down_tikv.log" \
        -C "$OUT_DIR/tikv-config.toml" \
        -s "$OUT_DIR/down_tikv" &

    sleep 5

    echo "Starting TiDB..."
    tidb-server \
        -P 4000 \
        -config "$OUT_DIR/tidb-config.toml" \
        --store tikv \
        --path 127.0.0.1:2379 \
        --log-file "$OUT_DIR/tidb.log" &

    echo "Verifying TiDB is started..."
    i=0
    while ! mysql -uroot -h127.0.0.1 -P4000 --default-character-set utf8 -e 'select * from mysql.tidb;'; do
        i=$((i+1))
        if [ "$i" -gt 40 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 3
    done

    echo "Starting Downstream TiDB..."
    tidb-server \
        -P 3306 \
        -config "$OUT_DIR/tidb-config.toml" \
        --store tikv \
        --path 127.0.0.1:2381 \
        --status=20080 \
        --log-file "$OUT_DIR/down_tidb.log" &

    echo "Verifying Downstream TiDB is started..."
    i=0
    while ! mysql -uroot -h127.0.0.1 -P3306 --default-character-set utf8 -e 'select * from mysql.tidb;'; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 3
    done

    echo "Starting CDC..."
    cdc server --log-file "$OUT_DIR/cdc.log" &
    sleep 1
}

trap stop_services EXIT
start_services

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

run_case() {
    local case=$1
    local script=$2
    echo "Running test $script..."
    PATH="$pwd/../bin:$pwd/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME=$case \
    sh "$script"
}

# List the case names to run, eg. ("cdc" "kafka")
do_cases=()

if [ ${#do_cases[@]} -eq 0 ]; then
    for script in ./*/run.sh; do
        test_name="$(basename "$(dirname "$script")")"
        run_case $test_name $script
    done
else
    for case in "${do_cases[@]}"; do
        script="./$case/run.sh"
        run_case $case $script
    done
fi

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
