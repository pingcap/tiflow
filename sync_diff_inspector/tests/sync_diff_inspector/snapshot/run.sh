
#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
FIX_DIR=/tmp/tidb_tools_test/sync_diff_inspector/fixsql
rm -rf $OUT_DIR
rm -rf $FIX_DIR
mkdir -p $OUT_DIR
mkdir -p $FIX_DIR

mysql -uroot -h 127.0.0.1 -P 4000 -e "show master status" > $OUT_DIR/ts.log
#cat $OUT_DIR/sync_diff.log
ts=`grep -oE "[0-9]+" $OUT_DIR/ts.log`
echo "get ts $ts"

echo "delete one data, diff should not passed"
mysql -uroot -h 127.0.0.1 -P 4000 -e "delete from diff_test.test limit 1"

sync_diff_inspector --config=./config_base.toml > $OUT_DIR/snapshot_diff.log || true
check_contains "check failed" $OUT_DIR/sync_diff.log
# move the fix sql file to $FIX_DIR
mv $OUT_DIR/fix-on-tidb/ $FIX_DIR/
rm -rf $OUT_DIR/*

echo "use snapshot compare data, test sql mode by the way, will auto discover ANSI_QUOTES thus pass"
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET GLOBAL sql_mode = 'ANSI_QUOTES';"
sleep 10
mysql -uroot -h 127.0.0.1 -P 4000 -e "show variables like '%sql_mode%'"
mysql -uroot -h 127.0.0.1 -P 4000 -e "show create table diff_test.test"
sed "s/#snapshot#/snapshot = \"${ts}\"/g" config_base.toml > config.toml
echo "use snapshot compare data, data should be equal"
sync_diff_inspector --config=./config.toml #> $OUT_DIR/snapshot_diff.log
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "execute fix.sql and use base config, and then compare data, data should be equal"
cat $FIX_DIR/fix-on-tidb/*.sql | mysql -uroot -h127.0.0.1 -P 4000
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/snapshot_diff.log
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

# reset sql mode
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET GLOBAL sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"

echo "snapshot test passed"