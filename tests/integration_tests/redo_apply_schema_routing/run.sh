#!/bin/bash

# [DESCRIPTION]:
#   This test verifies that redo log replay works correctly with schema routing.
#   It tests the following scenario:
#   1. Create a changefeed with redo log enabled AND schema routing (source_db -> target_db)
#   2. Insert data into source_db.t1 in upstream
#   3. Verify data flows to target_db.t1 in downstream (via schema routing)
#   4. Stop the changefeed
#   5. Clear downstream and use `cdc redo apply` to replay redo logs
#   6. Verify data is replayed to target_db.t1 (schema routing preserved in redo logs)

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

REDO_DIR="/tmp/tidb_cdc_test/redo_apply_schema_routing/redo"

function cleanup_redo_dir() {
    rm -rf $REDO_DIR
    mkdir -p $REDO_DIR
}

function run() {
    # Only run for mysql sink type since redo apply targets MySQL downstream
    if [ "$SINK_TYPE" != "mysql" ]; then
        echo "skip redo apply test for non-mysql sink"
        return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    cleanup_redo_dir

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # Create SOURCE database and table in upstream (where data originates)
    run_sql "CREATE DATABASE IF NOT EXISTS source_db;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE TABLE source_db.t1 (id INT PRIMARY KEY, val VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    # Create TARGET database and table in downstream (where data should be routed to)
    # The schema routing rule routes source_db.* -> target_db.*
    run_sql "CREATE DATABASE IF NOT EXISTS target_db;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql "CREATE TABLE target_db.t1 (id INT PRIMARY KEY, val VARCHAR(255));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    # Also create source_db in downstream to verify data does NOT go there
    run_sql "CREATE DATABASE IF NOT EXISTS source_db;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql "CREATE TABLE source_db.t1 (id INT PRIMARY KEY, val VARCHAR(255));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

    # Start CDC server
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    SINK_URI="mysql://root@127.0.0.1:3306/"
    changefeedid="redo-schema-routing-test"

    # Create changefeed with redo log AND schema routing enabled
    run_cdc_cli changefeed create \
        --start-ts=$start_ts \
        --sink-uri="$SINK_URI" \
        --changefeed-id=$changefeedid \
        --config="$CUR/conf/changefeed.toml"

    # Insert data into SOURCE database in upstream
    echo "Inserting data into source_db.t1..."
    for i in $(seq 1 50); do
        run_sql "INSERT INTO source_db.t1 VALUES ($i, 'value_$i');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    done

    # Wait for data to be replicated
    sleep 10

    # Verify data arrived in TARGET database (not source database) via schema routing
    echo "Verifying schema routing during normal replication..."
    upstream_count=$(run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT} | grep -oP '\d+' | tail -1)
    target_count=$(run_sql "SELECT COUNT(*) as cnt FROM target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} | grep -oP '\d+' | tail -1)
    source_count=$(run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} | grep -oP '\d+' | tail -1)

    echo "Upstream source_db.t1 count: $upstream_count"
    echo "Downstream target_db.t1 count: $target_count"
    echo "Downstream source_db.t1 count: $source_count"

    if [ "$target_count" != "$upstream_count" ]; then
        echo "ERROR: Schema routing not working - target_count ($target_count) != upstream_count ($upstream_count)"
        exit 1
    fi
    if [ "$source_count" -gt 0 ]; then
        echo "ERROR: Schema routing not working - data incorrectly in source_db.t1"
        exit 1
    fi
    echo "Schema routing verified: all $target_count rows correctly routed to target_db.t1"

    # Record the count before stopping
    pre_stop_count=$target_count

    # Stop the changefeed
    echo "Stopping changefeed..."
    run_cdc_cli changefeed pause --changefeed-id=$changefeedid || true
    sleep 2
    run_cdc_cli changefeed remove --changefeed-id=$changefeedid || true

    # Stop CDC server
    cleanup_process $CDC_BINARY

    # Clear downstream data to test redo apply from scratch
    echo "Clearing downstream data..."
    run_sql "TRUNCATE TABLE target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql "TRUNCATE TABLE source_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    # Verify downstream is empty before redo replay
    target_count_after_clear=$(run_sql "SELECT COUNT(*) as cnt FROM target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} | grep -oP '\d+' | tail -1)
    source_count_after_clear=$(run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} | grep -oP '\d+' | tail -1)
    echo "After clear - target_db.t1 count: $target_count_after_clear"
    echo "After clear - source_db.t1 count: $source_count_after_clear"

    if [ "$target_count_after_clear" != "0" ]; then
        echo "ERROR: target_db.t1 not empty after truncate ($target_count_after_clear rows)"
        exit 1
    fi
    if [ "$source_count_after_clear" != "0" ]; then
        echo "ERROR: source_db.t1 not empty after truncate ($source_count_after_clear rows)"
        exit 1
    fi
    echo "Downstream tables verified empty before redo replay"

    # Check if redo logs exist
    echo "Checking redo log files..."
    ls -la $REDO_DIR || echo "No redo files found"

    # Apply redo logs to downstream using cdc redo apply
    # The key test: redo logs should contain TargetSchema/TargetTable routing info
    echo "Applying redo logs to downstream..."
    if ls $REDO_DIR/*.log 1>/dev/null 2>&1 || ls $REDO_DIR/meta 1>/dev/null 2>&1; then
        # Run cdc redo apply
        $CDC_BINARY redo apply \
            --storage="file://$REDO_DIR" \
            --sink-uri="mysql://root@127.0.0.1:3306/?safe-mode=true" \
            2>&1 | tee $WORK_DIR/redo_apply.log || {
            echo "Redo apply failed, checking logs..."
            cat $WORK_DIR/redo_apply.log
            exit 1
        }

        echo "Redo apply completed"
    else
        echo "ERROR: No redo logs found to apply"
        exit 1
    fi

    # Verify schema routing was preserved through redo replay
    echo "Verifying schema routing after redo replay..."
    target_count_after_replay=$(run_sql "SELECT COUNT(*) as cnt FROM target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} | grep -oP '\d+' | tail -1)
    source_count_after_replay=$(run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} | grep -oP '\d+' | tail -1)

    echo "After redo replay - target_db.t1 count: $target_count_after_replay"
    echo "After redo replay - source_db.t1 count: $source_count_after_replay"
    echo "Expected count (from upstream): $upstream_count"

    # Verify data was replayed to TARGET database with correct count (schema routing preserved)
    if [ "$target_count_after_replay" != "$upstream_count" ]; then
        echo "ERROR: Redo replay count mismatch - target_count ($target_count_after_replay) != upstream_count ($upstream_count)"
        echo "This indicates TargetSchema/TargetTable were not preserved in redo logs"
        exit 1
    fi

    # Verify data did NOT go to source database
    if [ "$source_count_after_replay" -gt 0 ]; then
        echo "ERROR: Redo replay used wrong schema - data in source_db.t1 instead of target_db.t1"
        echo "This indicates TargetSchema/TargetTable were not used during redo apply"
        exit 1
    fi

    echo "SUCCESS: Schema routing preserved through redo log replay!"
    echo "  - All $target_count_after_replay rows correctly replayed to target_db.t1"
    echo "  - source_db.t1 correctly empty ($source_count_after_replay rows)"

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
