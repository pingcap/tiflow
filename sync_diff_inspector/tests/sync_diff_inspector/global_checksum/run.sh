#!/bin/sh

set -ex

cd "$(dirname "$0")"

OUT_DIR=/tmp/sync_diff_inspector_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

# Clustered PK case: success then fail.
mysql -uroot -h 127.0.0.1 -P 4000 <./data_clustered.sql
mysql -uroot -h 127.0.0.1 -P 4001 <./data_clustered.sql

echo "run global-checksum mode on clustered PK table"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=./config_clustered.toml >$OUT_DIR/global_checksum_clustered.output

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE
check_contains "\"column\":\"id\"" $CHECKPOINT_FILE
check_not_contains "_tidb_rowid" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "run global-checksum mode on clustered PK table with random splitter strategy"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/splitter/print-chunk-info=return();github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=./config_clustered_random.toml >$OUT_DIR/global_checksum_clustered_random.output

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
check_contains "random splitter" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "run global-checksum mode on clustered PK table with limit splitter strategy"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/splitter/print-chunk-info=return();github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=./config_clustered_limit.toml >$OUT_DIR/global_checksum_clustered_limit.output

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
check_contains "limit splitter" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "introduce mismatch, global-checksum mode should fail on clustered PK table"
mysql -uroot -h 127.0.0.1 -P 4000 -e "UPDATE checksum_mode_clustered.t SET v='z' WHERE id=1;"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=./config_clustered.toml >$OUT_DIR/global_checksum_clustered_fail.output || true

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

echo "run global-checksum mode on nonclustered PK table"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=./config_nonclustered.toml >$OUT_DIR/global_checksum_nonclustered.output

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE
check_contains "_tidb_rowid" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "introduce mismatch, global-checksum mode should fail on nonclustered PK table"
mysql -uroot -h 127.0.0.1 -P 4000 -e "UPDATE checksum_mode_nonclustered.t SET v='z' WHERE id=1;"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=./config_nonclustered.toml >$OUT_DIR/global_checksum_nonclustered_fail.output || true

check_contains "check failed" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_not_contains "chunk-info" $CHECKPOINT_FILE
check_contains "_tidb_rowid" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

# Reload matching data (previous test left a mismatch).
mysql -uroot -h 127.0.0.1 -P 4000 <./data_clustered.sql
mysql -uroot -h 127.0.0.1 -P 4001 <./data_clustered.sql

# ========== Partial checkpoint restart with random strategy ==========
# The restart test must keep the same config file before/after restart.
RANDOM_CFG=./config_clustered_random.toml
echo "inject source error to create a partial checkpoint (random strategy)"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/checksum-skip-chunk=return();github.com/pingcap/tiflow/sync_diff_inspector/diff/checksum-error-source=1*return();github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=$RANDOM_CFG >$OUT_DIR/partial_run.output || true

CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
# checksum-skip-chunk skips seq 2, so only chunks 0-1 are checkpointed.
# checksum-error-source errors one source. Both sides must be done:false.
check_contains "\"done\":false" $CHECKPOINT_FILE

echo "restart from partial checkpoint without error, random strategy should resume and pass"
rm -f $OUT_DIR/sync_diff.log
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=$RANDOM_CFG >$OUT_DIR/restart_run.output

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
# After successful restart both sides must be done.
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_not_contains "\"done\":false" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

# ========== Partial checkpoint restart with limit strategy ==========
# The restart test must keep the same config file before/after restart.
LIMIT_CFG=./config_clustered_limit.toml
echo "inject source error to create a partial checkpoint (limit strategy)"
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/checksum-skip-chunk=return();github.com/pingcap/tiflow/sync_diff_inspector/diff/checksum-error-source=1*return();github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=$LIMIT_CFG >$OUT_DIR/partial_run_limit.output || true

CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_contains "checksum-info" $CHECKPOINT_FILE
check_contains "\"done\":false" $CHECKPOINT_FILE

echo "restart from partial checkpoint without error, limit strategy should resume and pass"
rm -f $OUT_DIR/sync_diff.log
GO_FAILPOINTS="github.com/pingcap/tiflow/sync_diff_inspector/diff/wait-for-checkpoint=return()" \
	sync_diff_inspector --config=$LIMIT_CFG >$OUT_DIR/restart_run_limit.output

check_contains "check pass!!!" $OUT_DIR/sync_diff.log
CHECKPOINT_FILE=$OUT_DIR/checkpoint/sync_diff_checkpoints.pb
check_not_contains "\"done\":false" $CHECKPOINT_FILE

rm -rf $OUT_DIR/*

echo "global_checksum test passed"
