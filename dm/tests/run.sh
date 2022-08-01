#!/bin/bash

set -eu

TEST_DIR=/tmp/dm_test
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/_utils/env_variables

stop_services() {
	echo "..."
	# clean sql mode
	mysql -u root -h $MYSQL_HOST1 -P $MYSQL_PORT1 -p$MYSQL_PASSWORD1 -e "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
	mysql -u root -h $MYSQL_HOST2 -P $MYSQL_PORT2 -p$MYSQL_PASSWORD2 -e "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
}

print_worker_stacks() {
	if [ $? != 0 ]; then
		mkdir -p "$TEST_DIR/goroutines/stack/log"
		# don't know which case failed, so we print them all
		for port in $MASTER_PORT1 $MASTER_PORT2 $MASTER_PORT3 $MASTER_PORT4 $MASTER_PORT5 $MASTER_PORT6; do
			curl -sS "127.0.0.1:$port/debug/pprof/goroutine?debug=2" >"$TEST_DIR/goroutines/stack/log/master-$port.log" || true
		done
		for port in $WORKER1_PORT $WORKER2_PORT $WORKER3_PORT $WORKER4_PORT $WORKER5_PORT; do
			curl -sS "127.0.0.1:$port/debug/pprof/goroutine?debug=2" >"$TEST_DIR/goroutines/stack/log/worker-$port.log" || true
		done
	fi
}

check_mysql() {
	host=$1
	port=$2
	password=$3
	while ! mysql -u root -h ${host} -P ${port} -p${password} -e 'select version();'; do
		i=$((i + 1))
		if [ "$i" -gt 10 ]; then
			echo "wait for mysql ${host}:${port} timeout"
			exit 1
		fi
		sleep 1
	done
}

set_default_variables() {
	host=$1
	port=$2
	password=$3
	mysql -u root -h ${host} -P ${port} -p${password} -e "set global character_set_server='utf8mb4';set global collation_server='utf8mb4_bin';"
}

start_services() {
	stop_services

	mkdir -p "$TEST_DIR"
	rm -rf "$TEST_DIR/*.log"

	$CUR/_utils/run_tidb_server $TIDB_PORT $TIDB_PASSWORD

	i=0

	check_mysql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_mysql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	set_default_variables $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	set_default_variables $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
}

if [ "$#" -ge 1 ]; then
	test_case="$@"
else
	test_case="*"
fi

should_run=0
if [ "$test_case" == "*" ]; then
	should_run=1
elif [ "$test_case" == "compatibility" ]; then
	should_run=1
elif [ "$test_case" == "others" ]; then
	test_case=$(cat $CUR/others_integration_1.txt)
	should_run=1
elif [ "$test_case" == "others_2" ]; then
	test_case=$(cat $CUR/others_integration_2.txt)
	should_run=1
elif [ "$test_case" == "others_3" ]; then
	test_case=$(cat $CUR/others_integration_3.txt)
	if [ ! -z "$test_case" ]; then
		should_run=1
	fi
else
	exist_case=""
	for one_case in $test_case; do
		if [ ! -d "tests/$one_case" ]; then
			echo $one_case "not exist"
		else
			exist_case="$exist_case $one_case"
			should_run=1
		fi
	done
	test_case=$exist_case
fi

if [ $should_run -eq 0 ]; then
	exit 0
fi

trap stop_services EXIT
trap print_worker_stacks EXIT
start_services

function run() {
	script=$1
	echo "Running test $script..."
	# run in verbose mode?
	echo "Verbose mode = $VERBOSE"
	if $VERBOSE; then
		TEST_DIR="$TEST_DIR" \
			PATH="tests/_utils:$PATH" \
			TEST_NAME="$(basename "$(dirname "$script")")" \
			bash -x "$script"
	else
		TEST_DIR="$TEST_DIR" \
			PATH="tests/_utils:$PATH" \
			TEST_NAME="$(basename "$(dirname "$script")")" \
			bash +x "$script"
	fi
}

if [ "$test_case" == "*" ]; then
	for script in $CUR/$test_case/run.sh; do
		echo "start running case: [$test_case] script: [$script]"
		run $script
	done
elif [ "$test_case" == "compatibility" ]; then
	script="$CUR/compatibility/start.sh"
	echo "start running case: [$test_case] script: [$script]"
	run $script
else
	for name in $test_case; do
		script="$CUR/$name/run.sh"
		echo "start running case: [$name] script: [$script]"
		run $script
	done
fi
