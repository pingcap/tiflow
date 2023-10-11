#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=$1
group=$2

# Other tests that only support mysql: batch_update_to_no_batch ddl_reentrant
# changefeed_fast_fail changefeed_resume_with_checkpoint_ts sequence
# multi_cdc_cluster capture_suicide_while_balance_table
mysql_only="bdr_mode capture_suicide_while_balance_table syncpoint hang_sink_suicide"
mysql_only_http="http_api http_api_tls api_v2"
mysql_only_consistent_replicate="consistent_replicate_ddl consistent_replicate_gbk consistent_replicate_nfs consistent_replicate_storage_file consistent_replicate_storage_file_large_value consistent_replicate_storage_s3 consistent_partition_table"

kafka_only="kafka_big_messages kafka_compression kafka_messages kafka_sink_error_resume mq_sink_lost_callback"
kafka_only_protocol="canal_json_adapter_compatibility canal_json_basic multi_topics canal_json_handle_key_only open_protocol_handle_key_only"
kafka_only_v2="kafka_big_txn_v2 kafka_big_messages_v2 multi_tables_ddl_v2 multi_topics_v2"

storage_only_csv="csv_storage_basic csv_storage_multi_tables_ddl csv_storage_partition_table storage_csv_update"
storage_only_canal_json="canal_json_storage_basic canal_json_storage_partition_table lossy_ddl"

# Define groups
# Note: If new group is added, the group name must also be added to CI
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_cdc_integration_kafka_test.groovy
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_cdc_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
declare -A groups
groups=(
	# Note: only the tests in the first three groups are running in storage sink pipeline.
	["G00"]="$mysql_only $kafka_only $storage_only"
	["G01"]="$mysql_only_http $kafka_only_protocol $storage_only_canal_json multi_tables_ddl"
	["G02"]="$mysql_only_consistent_replicate $kafka_only_v2 $storage_only_csv"
	["G03"]='row_format drop_many_tables processor_stop_delay partition_table'
	["G04"]='foreign_key ddl_puller_lag ddl_only_block_related_table changefeed_auto_stop'
	["G05"]='charset_gbk ddl_manager multi_source'
	["G06"]='sink_retry changefeed_error ddl_sequence resourcecontrol'
	["G07"]='kv_client_stream_reconnect cdc split_region'
	["G08"]='server_config_compatibility processor_err_chan changefeed_reconstruct multi_capture'
	["G09"]='gc_safepoint changefeed_pause_resume cli savepoint'
	["G10"]='default_value simple cdc_server_tips event_filter'
	["G11"]='resolve_lock move_table autorandom generate_column'
	["G12"]='many_pk_or_uk capture_session_done_during_task ddl_attributes'
	["G13"]='tiflash new_ci_collation_without_old_value region_merge common_1'
	["G14"]='big_txn changefeed_finish force_replicate_table'
	["G15"]='new_ci_collation_with_old_value batch_add_table'
	# currently G16 is not running in kafka pipeline
	["G16"]='owner_resign processor_etcd_worker_delay sink_hang'
	["G17"]='clustered_index processor_resolved_ts_fallback'
	# only run the following tests in mysql pipeline
	["G18"]='availability http_proxies sequence'
	["G19"]='changefeed_fast_fail batch_update_to_no_batch changefeed_resume_with_checkpoint_ts'
	["G20"]='tidb_mysql_test ddl_reentrant multi_cdc_cluster multi_changefeed'
	["G21"]='bank kill_owner_with_ddl owner_remove_table_error'
	["G22"]=''
	["G23"]=''
	["G24"]=''
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
