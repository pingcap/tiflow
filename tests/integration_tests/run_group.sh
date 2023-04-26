#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=$1
group=$2

# Other tests that only support mysql: batch_update_to_no_batch ddl_reentrant
# changefeed_fast_fail changefeed_resume_with_checkpoint_ts sequence
# multi_cdc_cluster capture_suicide_while_balance_table
mysql_only="bdr_mode capture_suicide_while_balance_table syncpoint"
mysql_only_http="http_api http_api_tls api_v2"
mysql_only_consistent_replicate="consistent_replicate_ddl consistent_replicate_gbk consistent_replicate_nfs consistent_replicate_storage_file consistent_replicate_storage_s3"

# Tests that need to support kafka: bank kill_owner_with_ddl owner_remove_table_error
# owner_resign processor_etcd_worker_delay processor_resolved_ts_fallback
# multi_changefeed clustered_index sink_hang
kafka_only="kafka_big_messages kafka_compression kafka_messages kafka_sink_error_resume"
kafka_only_protocol="canal_json_adapter_compatibility canal_json_basic multi_topics"
kafka_only_v2="big_txn_v2 kafka_big_messages_v2 multi_tables_ddl_v2 multi_topics_v2"

storage_only_csv="csv_storage_basic csv_storage_multi_tables_ddl csv_storage_partition_table"
storage_only_canal_json="canal_json_storage_basic canal_json_storage_partition_table"

# Define groups
# Note: If new group is added, the group name must also be added to CI
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_cdc_integration_kafka_test.groovy
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_cdc_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
declare -A groups
groups=(
	["G00"]="$mysql_only $kafka_only $storage_only_csv"
	["G01"]="$mysql_only_http $kafka_only_protocol $storage_only_canal_json"
	["G02"]="$mysql_only_consistent_replicate $kafka_only_v2"
	["G03"]='row_format drop_many_tables processor_stop_delay'
	["G04"]='foreign_key ddl_puller_lag'
	["G05"]='partition_table changefeed_auto_stop'
	["G06"]='charset_gbk owner_remove_table_error'
	["G07"]='clustered_index multi_tables_ddl'
	["G08"]='bank multi_source multi_capture'
	["G09"]='ddl_reentrant multi_cdc_cluster'
	["G10"]='sink_retry changefeed_error ddl_sequence'
	["G11"]='kv_client_stream_reconnect cdc default_value'
	["G12"]='changefeed_fast_fail tidb_mysql_test server_config_compatibility'
	["G13"]='resourcecontrol processor_etcd_worker_delay'
	["G14"]='batch_update_to_no_batch gc_safepoint changefeed_pause_resume'
	["G15"]='cli simple cdc_server_tips changefeed_resume_with_checkpoint_ts'
	["G16"]='processor_err_chan resolve_lock move_table autorandom'
	["G17"]='ddl_attributes many_pk_or_uk capture_session_done_during_task'
	["G18"]='tiflash new_ci_collation_without_old_value region_merge common_1'
	["G19"]='split_region availability kill_owner_with_ddl'
	["G20"]='changefeed_reconstruct http_proxies savepoint'
	["G21"]='event_filter generate_column sequence processor_resolved_ts_fallback'
	["G22"]='big_txn changefeed_finish sink_hang'
	["G23"]='new_ci_collation_with_old_value batch_add_table '
	["G24"]='owner_resign force_replicate_table multi_changefeed'
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

if [[ "$group" == "others" ]]; then
	if [[ -z $others ]]; then
		echo "All CDC integration test cases are added to groups"
		exit 0
	fi
	echo "Error: "$others" is not added to any group in tests/integration_tests/run_group.sh"
	exit 1
elif [[ " ${!groups[*]} " =~ " ${group} " ]]; then
	test_names="${groups[${group}]}"
	# Run test cases
	if [[ -n $test_names ]]; then
		echo "Run cases: ${test_names}"
		"${CUR}"/run.sh "${sink_type}" "${test_names}"
	fi
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi
