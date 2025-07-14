#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=$1
group=$2
group_num=${group#G}

# Other tests that only support mysql: batch_update_to_no_batch ddl_reentrant
# changefeed_fast_fail changefeed_resume_with_checkpoint_ts sequence
# multi_cdc_cluster capture_suicide_while_balance_table
mysql_only="down_db_require_secure_transport bdr_mode capture_suicide_while_balance_table syncpoint syncpoint_check_ts server_config_compatibility changefeed_dup_error_restart safe_mode"
mysql_only_http="http_api http_api_tls api_v2 http_api_tls_with_user_auth cli_tls_with_auth"
mysql_only_consistent_replicate="consistent_replicate_ddl consistent_replicate_gbk consistent_replicate_nfs consistent_replicate_storage_file consistent_replicate_storage_file_large_value consistent_replicate_storage_s3 consistent_partition_table"

kafka_only="kafka_big_messages kafka_compression kafka_messages kafka_sink_error_resume mq_sink_lost_callback mq_sink_dispatcher kafka_column_selector kafka_column_selector_avro debezium"
kafka_only_protocol="debezium_basic kafka_simple_basic kafka_simple_basic_avro kafka_simple_handle_key_only kafka_simple_handle_key_only_avro kafka_simple_claim_check kafka_simple_claim_check_avro canal_json_adapter_compatibility canal_json_basic canal_json_content_compatible multi_topics avro_basic canal_json_handle_key_only open_protocol_handle_key_only canal_json_claim_check open_protocol_claim_check"
kafka_only_v2="kafka_big_messages_v2 multi_tables_ddl_v2 multi_topics_v2"

storage_only="lossy_ddl storage_csv_update csv_storage_update_pk_clustered csv_storage_update_pk_nonclustered"
storage_only_csv="storage_cleanup csv_storage_basic csv_storage_multi_tables_ddl csv_storage_partition_table"
storage_only_canal_json="canal_json_storage_basic canal_json_storage_partition_table"

# Define groups
# Note: If new group is added, the group name must also be added to CI
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_cdc_integration_kafka_test.groovy
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tiflow/latest/pull_cdc_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
groups=(
	# Note: only the tests in the first three groups are running in storage sink pipeline.
	# G00
	"debezium"
	# G01
	""
	# G02
	""
	# G03
	''
	# G04
	''
	# G05
	''
	# G06
	''
	# G07 pulsar oauth2 authentication enabled
	''
	# G08
	''
	# G09
	''
	# G10
	''
	# G11
	''
	# G12
	''
	# G13 pulsar mtls authentication enabled
	''
	# G14
	''
	# G15
	''
	# G16, currently G16 is not running in kafka pipeline
	''
	# G17
	''
	# only run the following tests in mysql pipeline
	# G18
	''
	# G19
	''
	# G20
	''
	# G21
	''
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
elif [[ $group_num =~ ^[0-9]+$ ]] && [[ -n ${groups[10#${group_num}]} ]]; then
	# force use decimal index
	test_names="${groups[10#${group_num}]}"
	# Run test cases
	echo "Run cases: ${test_names}"
	"${CUR}"/run.sh "${sink_type}" "${test_names}"
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi
