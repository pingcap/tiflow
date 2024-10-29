#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

group=$1
group_num=${group#G}

# Define groups
# Note: If new group is added, the group name must also be added to CI
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_engine_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
groups=(
	# G00
	'dm_basic dm_case_sensitive dm_collation dm_dump_sync_mode'
	# G01
	'dm_full_mode dm_lightning_checkpoint dm_many_tables'
	# G02
	"dm_many_tables_local dm_new_collation_off dm_sql_mode"
	# G03
	"dm_tls e2e_fast_finished e2e_node_failure e2e_with_selectors"
	# G04
	"e2e_worker_error external_resource"
)

# Get other cases not in groups, to avoid missing any case
others=()
for script in "$CUR"/*/run.sh; do
	test_name="$(basename "$(dirname "$script")")"
	# shellcheck disable=SC2076
	if [[ ! " ${groups[*]} " =~ " ${test_name} " ]]; then
		others=("${others[@]} ${test_name}")
	fi
done

if [[ "$group" == "check others" ]]; then
	if [[ -z $others ]]; then
		echo "All engine integration test cases are added to groups"
		exit 0
	fi
	echo "Error: "$others" is not added to any group in engine/test/integration_tests/run_group.sh"
	exit 1
elif [[ $group_num =~ ^[0-9]+$ ]] && [[ -n ${groups[10#${group_num}]} ]]; then
	# force use decimal index
	test_names="${groups[10#${group_num}]}"
	# Run test cases
	echo "Run cases: ${test_names}"
	mkdir -p /tmp/tiflow_engine_test
	"${CUR}"/run.sh "${test_names}" 2>&1 | tee /tmp/tiflow_engine_test/engine_it.log
	./engine/test/utils/check_log.sh
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi
