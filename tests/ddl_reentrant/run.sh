#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

ddls=("create database ddl_reentrant" false
      "create table ddl_reentrant.t1 (id int primary key, id2 int not null, a varchar(10) not null, unique a(a), unique id2(id2))" false
      "alter table ddl_reentrant.t1 add column b int" false
      "alter table ddl_reentrant.t1 drop column b" false
      "alter table ddl_reentrant.t1 add key index_a(a)" false
      "alter table ddl_reentrant.t1 drop index index_a" false
      "truncate table ddl_reentrant.t1" true
      "alter table ddl_reentrant.t1 modify a varchar(20)" true
      "rename table ddl_reentrant.t1 to ddl_reentrant.t2" false
      "alter table ddl_reentrant.t2 alter a set default 'hello'" true
      "alter table ddl_reentrant.t2 comment='modify comment'" true
      "alter table ddl_reentrant.t2 rename index a to idx_a" false
      "create table ddl_reentrant.t3 (a int primary key, b int) partition by range(a) (partition p0 values less than (1000), partition p1 values less than (2000))" false
      "alter table ddl_reentrant.t3 add partition (partition p2 values less than (3000))" false
      "alter table ddl_reentrant.t3 drop partition p2" false
      "alter table ddl_reentrant.t3 truncate partition p0" true
      "create view ddl_reentrant.t3_view as select a, b from ddl_reentrant.t3" false
      "drop view ddl_reentrant.t3_view" false
      "alter table ddl_reentrant.t3 default character set utf8mb4 default collate utf8mb4_unicode_ci" true
      "alter schema ddl_reentrant default character set utf8mb4 default collate utf8mb4_unicode_ci" true
)

function complete_ddls() {
    tidb_build_branch=$(mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -e \
        "select tidb_version()\G"|grep "Git Branch"|awk -F: '{print $(NF)}'|tr -d " ")
    if [[ ! $tidb_build_branch =~ master ]]; then
        echo "skip some DDLs in tidb v4.0.x"
    else
        # DDLs that are supportted since 5.0
        ddls+=( "alter table ddl_reentrant.t2 add column c1 int, add column c2 int, add column c3 int" false )
        ddls+=( "alter table ddl_reentrant.t2 drop column c1, drop column c2, drop column c3" false )
    fi
    ddls+=( "alter table ddl_reentrant.t2 drop primary key" false )
    ddls+=( "alter table ddl_reentrant.t2 add primary key pk(id)" false )
    ddls+=( "drop table ddl_reentrant.t2" false )
    ddls+=( "recover table ddl_reentrant.t2" false )
    ddls+=( "drop database ddl_reentrant" false )
}

changefeedid=""
SINK_URI="mysql://root@127.0.0.1:3306/"

function check_ts_forward() {
    changefeedid=$1
    rts1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."resolved-ts"')
    checkpoint1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."checkpoint-ts"')
    sleep 1
    rts2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."resolved-ts"')
    checkpoint2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."checkpoint-ts"')
    if [[ "$rts1" != "null" ]] && [[ "$rts1" != "0" ]]; then
        if [[  "$rts1" -ne "$rts2" ]] || [[ "$checkpoint1" -ne "$checkpoint2" ]]; then
            echo "changefeed is working normally rts: ${rts1}->${rts2} checkpoint: ${checkpoint1}->${checkpoint2}"
            return
        fi
    fi
    exit 1
}

function check_ddl_executed() {
    log_file="$1"
    ddl=$(cat $2)
    success="$3"
    if [[ $success == "true" ]]; then
        key_word="Exec DDL succeeded"
    else
        key_word="execute DDL failed, but error can be ignored"
    fi
    log=$(grep "${key_word}" ${log_file}|tail -n 1)
    if [[ $log == *"${ddl}"* ]]; then
        echo $log
        return
    else
        exit 1
    fi
}

export -f check_ts_forward
export -f check_ddl_executed

function ddl_test() {
    ddl=$1
    is_reentrant=$2

    echo "------------------------------------------"
    echo "test ddl $ddl, is_reentrant: $is_reentrant"

    run_sql $ddl ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    ensure 10 check_ts_forward $changefeedid

    echo $ddl > ${WORK_DIR}/ddl_temp.sql
    ensure 10 check_ddl_executed "${WORK_DIR}/cdc.log" "${WORK_DIR}/ddl_temp.sql" true
    ddl_start_ts=$(grep "Execute DDL succeeded" ${WORK_DIR}/cdc.log|tail -n 1|grep -oE '"start_ts\\":[0-9]{18}'|awk -F: '{print $(NF)}')
    cdc cli changefeed remove --changefeed-id=${changefeedid}
    changefeedid=$(cdc cli changefeed create --no-confirm --start-ts=${ddl_start_ts} --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    echo "create new changefeed ${changefeedid} from ${ddl_start_ts}"
    ensure 10 check_ts_forward $changefeedid
    ensure 10 check_ddl_executed "${WORK_DIR}/cdc.log" "${WORK_DIR}/ddl_temp.sql" $is_reentrant
}

function run() {
    # don't test kafka in this case
    if [ "$SINK_TYPE" == "kafka" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR --tidb-config $CUR/conf/tidb_config.toml

    complete_ddls

    cd $WORK_DIR

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
    changefeedid=$(cdc cli changefeed create --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')

    OLDIFS=$IFS
    IFS=""
    idx=0
    while [ $idx -lt ${#ddls[*]} ]; do
        ddl=${ddls[$idx]}
        idx=$((idx+1))
        idxs_reentrant=${ddls[$idx]}
        idx=$((idx+1))
        ddl_test $ddl $idxs_reentrant
    done
    IFS=$OLDIFS

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
