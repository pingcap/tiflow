#!/bin/sh

set -ex

cd "$(dirname "$0")"

OUT_DIR=/tmp/sync_diff_inspector_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

# Clustered PK case: success then fail.
mysql -uroot -h 127.0.0.1 -P 4000 <./data_clustered.sql
mysql -uroot -h 127.0.0.1 -P 4001 <./data_clustered.sql

echo "run checksum-only mode on clustered PK table"
export GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config_clustered.toml >$OUT_DIR/checksum_only_clustered.output
export GO_FAILPOINTS=""

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE
check_contains "\"column\":\"id\"" $CHECKPOINT_FILE
check_not_contains "_tidb_rowid" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "introduce mismatch, checksum-only mode should fail on clustered PK table"
mysql -uroot -h 127.0.0.1 -P 4000 -e "UPDATE checksum_mode_clustered.t SET v='z' WHERE id=1;"
export GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config_clustered.toml >$OUT_DIR/checksum_only_clustered_fail.output || true
export GO_FAILPOINTS=""

check_contains "check failed" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE
check_contains "\"column\":\"id\"" $CHECKPOINT_FILE
check_not_contains "_tidb_rowid" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

# Nonclustered PK case: success then fail.
mysql -uroot -h 127.0.0.1 -P 4000 <./data_nonclustered.sql
mysql -uroot -h 127.0.0.1 -P 4001 <./data_nonclustered.sql

# Ensure _tidb_rowid exists for NONCLUSTERED PK table.
mysql -uroot -h 127.0.0.1 -P 4000 -e "SELECT _tidb_rowid FROM checksum_mode_nonclustered.t LIMIT 1;" >$OUT_DIR/nonclustered_rowid.output

echo "run checksum-only mode on nonclustered PK table"
export GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config_nonclustered.toml >$OUT_DIR/checksum_only_nonclustered.output
export GO_FAILPOINTS=""

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE
check_contains "_tidb_rowid" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "introduce mismatch, checksum-only mode should fail on nonclustered PK table"
mysql -uroot -h 127.0.0.1 -P 4000 -e "UPDATE checksum_mode_nonclustered.t SET v='z' WHERE id=1;"
export GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config_nonclustered.toml >$OUT_DIR/checksum_only_nonclustered_fail.output || true
export GO_FAILPOINTS=""

check_contains "check failed" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE
check_contains "_tidb_rowid" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "checksum_only test passed"
