#!/bin/sh

set -ex

cd "$(dirname "$0")"
OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
FIX_DIR=/tmp/tidb_tools_test/sync_diff_inspector/fixsql
rm -rf $OUT_DIR
rm -rf $FIX_DIR
mkdir -p $OUT_DIR
mkdir -p $FIX_DIR

for port in 4000 4001; do
  mysql -uroot -h 127.0.0.1 -P $port -e "create database if not exists expression_test;"
  mysql -uroot -h 127.0.0.1 -P $port -e "create table expression_test.diff(\`a\`\`;sad\` int, id int);"
  mysql -uroot -h 127.0.0.1 -P $port -e "alter table expression_test.diff add index i1((\`a\`\`;sad\` + 1 + \`a\`\`;sad\`));"
  mysql -uroot -h 127.0.0.1 -P $port -e "insert into expression_test.diff values (1,1),(2,2),(3,3);"
done

echo "check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/expression_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*
