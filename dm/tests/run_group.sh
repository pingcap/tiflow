#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

group=$1
if [[ $group == "TLS_GROUP" ]]; then
	group="G12"
fi
group_num=${group#G}

# Define groups
# Note: If new group is added, the group name must also be added to CI
#  https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_dm_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
groups=(
	# G00
	"ha_cases_1 ha_cases_2 ha_cases2"
	# G01
	"ha_cases3 ha_cases3_1 ha_master tracker_ignored_ddl validator_checkpoint incompatible_ddl_changes"
	# G02
	"handle_error handle_error_2 handle_error_3 adjust_gtid async_checkpoint_flush binlog_parse case_sensitive checkpoint_transaction check_task"
	# G03
	"dmctl_advance dmctl_basic dmctl_command downstream_diff_index downstream_more_column"
	# G04
	"import_goroutine_leak incremental_mode initial_unit drop_column_with_index duplicate_event expression_filter extend_column fake_rotate_event"
	# G05
	"load_interrupt many_tables online_ddl foreign_key full_mode gbk gtid ha_cases http_proxies"
	# G06
	"relay_interrupt safe_mode sequence_safe_mode lightning_load_task lightning_mode metrics"
	# G07
	"shardddl1 shardddl1_1 shardddl2 shardddl2_1"
	# G08
	"shardddl3 shardddl3_1 shardddl4 shardddl4_1 sharding sequence_sharding sequence_sharding_removemeta"
	# G09
	"import_v10x sharding2 ha new_collation_off only_dml openapi s3_dumpling_lightning sequence_sharding_optimistic"
	# G10
	"start_task print_status http_apis new_relay all_mode"
	# `others others_2 others_3` tests of old pipeline
	# G11
	"validator_basic dm_syncer shardddl_optimistic slow_relay_writer sql_mode sync_collation"
	# G12 TLS_GROUP
	"tls"
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
	echo "Error: "$others" is not added to any group in dm/tests/run_group.sh"
	exit 1
elif [[ $group_num =~ ^[0-9]+$ ]] && [[ -n ${groups[10#${group_num}]} ]]; then
	# force use decimal index
	test_names="${groups[10#${group_num}]}"
	# Run test cases
	echo "Run cases: ${test_names}"
	"${CUR}"/run.sh "${test_names}"
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi
