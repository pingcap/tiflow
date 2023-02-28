#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=$1
group=$2

# Define groups
# Note: If new group is added, the group name must also be added to CI
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/tikv/migration/latest/pull_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
declare -A groups
groups=(
	["G00"]='changefeed_error ddl_sequence force_replicate_table'
	["G01"]='multi_capture kafka_big_messages cdc'
	["G02"]='drop_many_tables multi_cdc_cluster processor_stop_delay'
	["G03"]='capture_suicide_while_balance_table row_format'
	["G04"]='foreign_key canal_json_basic ddl_puller_lag'
	["G05"]='partition_table changefeed_auto_stop sorter'
	["G06"]='charset_gbk owner_remove_table_error bdr_mode'
	["G07"]='clustered_index multi_tables_ddl big_txn_v2'
	["G08"]='bank multi_source kafka_sink_error_resume'
	["G09"]='capture_suicide_while_balance_table'
	["G10"]='multi_topics_v2 consistent_replicate_storage_s3 sink_retry'
	["G11"]='consistent_replicate_storage_file kv_client_stream_reconnect consistent_replicate_gbk'
	["G12"]='http_api changefeed_fast_fail tidb_mysql_test server_config_compatibility'
	["G13"]='canal_json_adapter_compatibility resourcecontrol processor_etcd_worker_delay'
	["G14"]='batch_update_to_no_batch gc_safepoint default_value changefeed_pause_resume'
	["G15"]='cli simple cdc_server_tips changefeed_resume_with_checkpoint_ts ddl_reentrant'
	["G16"]='processor_err_chan resolve_lock move_table kafka_compression autorandom'
	["G17"]='ddl_attributes many_pk_or_uk kafka_messages capture_session_done_during_task http_api_tls'
	["G18"]='tiflash new_ci_collation_without_old_value region_merge common_1'
	["G19"]='kafka_big_messages_v2 multi_tables_ddl_v2 split_region availability'
	["G20"]='changefeed_reconstruct http_proxies kill_owner_with_ddl savepoint'
	["G21"]='event_filter generate_column syncpoint sequence processor_resolved_ts_fallback'
	["G22"]='big_txn csv_storage_basic changefeed_finish sink_hang canal_json_storage_basic'
	["G23"]='multi_topics new_ci_collation_with_old_value batch_add_table multi_changefeed'
	["G24"]='consistent_replicate_nfs processor_panic owner_resign '
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
