#!/bin/bash

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$( cd $CUR/../_certificates && pwd )

function check_changefeed_state() {
    changefeedid=$1
    expected=$2
    output=$(cdc cli changefeed query --simple --changefeed-id $changefeedid --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1 2>&1)
    state=$(echo $output | grep -oE "\"state\": \"[a-z]+\""|tr -d '" '|awk -F':' '{print $(NF)}')
    if [ "$state" != "$expected" ]; then
        echo "unexpected state $output, expected $expected"
        exit 1
    fi
}

function check_changefeed_count() {
    pd_addr=$1
    expected=$2
    feed_count=$(cdc cli changefeed list --pd=$pd_addr|jq '.|length')
    if [[ "$feed_count" != "$expected" ]]; then
        echo "[$(date)] <<<<< unexpect changefeed count! expect ${expected} got ${feed_count} >>>>>"
        exit 1
    fi
    echo "changefeed count ${feed_count} check pass, pd_addr: $pd_addr"
}

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
    run_sql "CREATE table test.simple(id int primary key, val int);"
    run_sql "CREATE table test.\`simple-dash\`(id int primary key, val int);"

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-cli-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac

    uuid="custom-changefeed-name"
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --tz="Asia/Shanghai" -c="$uuid"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/"

    # Make sure changefeed is created.
    check_table_exists tidb_cdc.repl_mark_test_simple ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists tidb_cdc."\`repl_mark_test_simple-dash\`" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    check_changefeed_state $uuid "normal"

    check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1} 1
    check_changefeed_count http://${UP_PD_HOST_2}:${UP_PD_PORT_2} 1
    check_changefeed_count http://${UP_PD_HOST_3}:${UP_PD_PORT_3} 1
    check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1},http://${UP_PD_HOST_2}:${UP_PD_PORT_2},http://${UP_PD_HOST_3}:${UP_PD_PORT_3} 1

    # Make sure changefeed can not be created if the name is already exists.
    exists=$(run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="$uuid" 2>&1 | grep -oE 'already exists')
    if [[ -z $exists ]]; then
        echo "[$(date)] <<<<< unexpect output got ${exists} >>>>>"
        exit 1
    fi

    # Pause changefeed
    run_cdc_cli changefeed --changefeed-id $uuid pause && sleep 3
    jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
    if [[ $jobtype != 1 ]]; then
        echo "[$(date)] <<<<< unexpect admin job type! expect 1 got ${jobtype} >>>>>"
        exit 1
    fi
    check_changefeed_state $uuid "stopped"

    # Update changefeed
cat - >"$WORK_DIR/changefeed.toml" <<EOF
case-sensitive = false
[mounter]
worker-num = 4
EOF
    run_cdc_cli changefeed update --start-ts=$start_ts --sink-uri="$SINK_URI" --tz="Asia/Shanghai" --config="$WORK_DIR/changefeed.toml" --no-confirm --changefeed-id $uuid
    changefeed_info=$(run_cdc_cli changefeed query --changefeed-id $uuid 2>&1)
    if [[ ! $changefeed_info == *"\"case-sensitive\": false"* ]]; then
        echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
        exit 1
    fi
    if [[ ! $changefeed_info == *"\"worker-num\": 4"* ]]; then
        echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
        exit 1
    fi

    jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
    if [[ $jobtype != 1 ]]; then
        echo "[$(date)] <<<<< unexpect admin job type! expect 1 got ${jobtype} >>>>>"
        exit 1
    fi

    # Resume changefeed
    run_cdc_cli changefeed --changefeed-id $uuid resume && sleep 3
    jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
    if [[ $jobtype != 2 ]]; then
        echo "[$(date)] <<<<< unexpect admin job type! expect 2 got ${jobtype} >>>>>"
        exit 1
    fi
    check_changefeed_state $uuid "normal"

    # Remove changefeed
    run_cdc_cli changefeed --changefeed-id $uuid remove && sleep 3
    jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
    if [[ $jobtype != 3 ]]; then
        echo "[$(date)] <<<<< unexpect admin job type! expect 3 got ${jobtype} >>>>>"
        exit 1
    fi
    check_changefeed_state $uuid "removed"

    # Make sure bad sink url fails at creating changefeed.
    badsink=$(run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="mysql://badsink" 2>&1 | grep -oE 'fail')
    if [[ -z $badsink ]]; then
        echo "[$(date)] <<<<< unexpect output got ${badsink} >>>>>"
        exit 1
    fi

    # Test Kafka SSL connection.
    if [ "$SINK_TYPE" == "kafka" ]; then
        SSL_TOPIC_NAME="ticdc-cli-test-ssl-$RANDOM"
        SINK_URI="kafka://127.0.0.1:9093/$SSL_TOPIC_NAME?ca=${TLS_DIR}/ca.pem&cert=${TLS_DIR}/client.pem&key=${TLS_DIR}/client-key.pem"
        run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --tz="Asia/Shanghai"
    fi

    # Smoke test meta delete-gc-ttl and delete
    echo "y" | run_cdc_cli meta delete-gc-ttl
    run_cdc_cli meta delete --no-confirm

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
