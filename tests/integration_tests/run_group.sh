#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=$1
group=$2

# Define groups
# Note: If new group is added, the group name must also be added to CI
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/tikv/migration/latest/pull_integration_test.groovy
declare -A groups
groups=(
	["G00"]='autorandom'
	["G01"]='changefeed_error'
	["G02"]='ddl_sequence'
	["G03"]='force_replicate_table'
	["G04"]='multi_capture'
	["G05"]='kafka_big_messages'
	["G06"]='drop_many_tables'
	["G07"]='multi_cdc_cluster'
	["G08"]='processor_stop_delay'
	["G09"]='capture_suicide_while_balance_table'
	["G10"]='row_format'
	["G11"]='clustered_index'
	["G12"]='multi_source' # heavy test case
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
	"${CUR}"/run.sh "${sink_type}" "${test_names}"
fi
