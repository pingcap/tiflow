#!/bin/sh

set -e

cd "$(dirname "%0")"


run_sql "CREATE table test.simple1(id int primary key, val int);"
run_sql "CREATE table test.simple2(id int primary key, val int);"

# test insert/update/delete for two table in the same way.
run_sql "INSERT INTO test.simple1(id, val) VALUES (1, 1);"
run_sql "INSERT INTO test.simple1(id, val) VALUES (2, 2);"
run_sql "INSERT INTO test.simple1(id, val) VALUES (3, 3);"

# update id = 2 and delete id = 3
run_sql "UPDATE test.simple1 set val = 22 where id = 2;"
run_sql "DELETE from test.simple1 where id = 3;"


# same dml for table simple2
run_sql "INSERT INTO test.simple2(id, val) VALUES (1, 1);"
run_sql "INSERT INTO test.simple2(id, val) VALUES (2, 2);"
run_sql "INSERT INTO test.simple2(id, val) VALUES (3, 3);"

run_sql "UPDATE test.simple2 set val = 22 where id = 2;"
run_sql "DELETE from test.simple2 where id = 3;"

sleep 15
# check table simple1.
down_run_sql "SELECT id, val FROM test.simple1;"
check_contains "id: 1"
check_contains "val: 1"
check_contains "id: 2"
check_contains "val: 22"
check_not_contains "id: 3"

# check table simple2.
down_run_sql "SELECT id, val FROM test.simple2;"
check_contains "id: 1"
check_contains "val: 1"
check_contains "id: 2"
check_contains "val: 22"
check_not_contains "id: 3"

run_sql "DROP TABLE test.simple1;"
run_sql "DROP TABLE test.simple2;"


