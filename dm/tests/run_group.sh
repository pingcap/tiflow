#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

group=$1

# Define groups
# Note: If new group is added, the group name must also be added to CI
#  https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_dm_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
declare -A groups
groups=(
	["G00"]="ha_cases_1 ha_cases_2 ha_cases2"
	["G01"]="ha_cases3 ha_cases3_1 ha_master"
	["G02"]="handle_error handle_error_2 handle_error_3"
	["G03"]="dmctl_advance dmctl_basic dmctl_command"
	["G04"]="import_goroutine_leak incremental_mode initial_unit"
	["G05"]="load_interrupt many_tables online_ddl"
	["G06"]="relay_interrupt safe_mode sequence_safe_mode"
	["G07"]="shardddl1 shardddl1_1 shardddl2 shardddl2_1"
	["G08"]="shardddl3 shardddl3_1 shardddl4 shardddl4_1 sharding sequence_sharding"
	["G09"]="others others_2 others_3"
	["G10"]="start_task print_status http_apis new_relay all_mode"
	["G11"]="import_v10x sharding2 ha"
	["TLS_GROUP"]="tls"
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

# Get test names
test_names=""
# shellcheck disable=SC2076
if [[ "$group" == "others" ]]; then
	test_names="${others[*]}"
elif [[ " ${!groups[*]} " =~ " ${group} " ]]; then
	test_names="${groups[${group}]}"
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi

# Run test cases
if [[ -n $test_names ]]; then
	echo "Run cases: ${test_names}"
	"${CUR}"/run.sh "${test_names}"
fi
