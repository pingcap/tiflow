#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# cdc parse and restore ddl with flags format.RestoreStringSingleQuotes|format.RestoreNameBackQuotes|format.RestoreKeyWordUppercase|format.RestoreTiDBSpecialComment
ddls=("create database ddl_reentrant" false 'CREATE DATABASE `ddl_reentrant`'
	"create table ddl_reentrant.t1 (id int primary key, id2 int not null, a varchar(10) not null, unique a(a), unique id2(id2))" false 'CREATE TABLE `ddl_reentrant`.`t1` (`id` INT PRIMARY KEY,`id2` INT NOT NULL,`a` VARCHAR(10) NOT NULL,UNIQUE `a`(`a`),UNIQUE `id2`(`id2`))'
	"alter table ddl_reentrant.t1 add column b int" false 'ALTER TABLE `ddl_reentrant`.`t1` ADD COLUMN `b` INT'
	"alter table ddl_reentrant.t1 drop column b" false 'ALTER TABLE `ddl_reentrant`.`t1` DROP COLUMN `b`'
	"alter table ddl_reentrant.t1 add key index_a(a)" false 'ALTER TABLE `ddl_reentrant`.`t1` ADD INDEX `index_a`(`a`)'
	"alter table ddl_reentrant.t1 drop index index_a" false 'ALTER TABLE `ddl_reentrant`.`t1` DROP INDEX `index_a`'
	"truncate table ddl_reentrant.t1" true 'TRUNCATE TABLE `ddl_reentrant`.`t1`'
	"alter table ddl_reentrant.t1 modify a varchar(20)" true 'ALTER TABLE `ddl_reentrant`.`t1` MODIFY COLUMN `a` VARCHAR(20)'
	"rename table ddl_reentrant.t1 to ddl_reentrant.t2" false 'RENAME TABLE `ddl_reentrant`.`t1` TO `ddl_reentrant`.`t2`'
	"alter table ddl_reentrant.t2 alter a set default 'hello'" true 'ALTER TABLE `ddl_reentrant`.`t2` ALTER COLUMN `a` SET DEFAULT _UTF8MB4'"'hello'"
	"alter table ddl_reentrant.t2 comment='modify comment'" true 'ALTER TABLE `ddl_reentrant`.`t2` COMMENT = '"'modify comment'"
	"alter table ddl_reentrant.t2 rename index a to idx_a" false 'ALTER TABLE `ddl_reentrant`.`t2` RENAME INDEX `a` TO `idx_a`'
	"create table ddl_reentrant.t3 (a int primary key, b int) partition by range(a) (partition p0 values less than (1000), partition p1 values less than (2000))" false 'CREATE TABLE `ddl_reentrant`.`t3` (`a` INT PRIMARY KEY,`b` INT) PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (1000),PARTITION `p1` VALUES LESS THAN (2000))'
	"alter table ddl_reentrant.t3 add partition (partition p2 values less than (3000))" false 'ALTER TABLE `ddl_reentrant`.`t3` ADD PARTITION (PARTITION `p2` VALUES LESS THAN (3000))'
	"alter table ddl_reentrant.t3 drop partition p2" false 'ALTER TABLE `ddl_reentrant`.`t3` DROP PARTITION `p2`'
	"alter table ddl_reentrant.t3 truncate partition p0" true 'ALTER TABLE `ddl_reentrant`.`t3` TRUNCATE PARTITION `p0`'
	"create view ddl_reentrant.t3_view as select a, b from ddl_reentrant.t3" false 'CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `ddl_reentrant`.`t3_view` AS SELECT `a`,`b` FROM `ddl_reentrant`.`t3`'
	"drop view ddl_reentrant.t3_view" false 'DROP VIEW `ddl_reentrant`.`t3_view`'
	"alter table ddl_reentrant.t3 default character set utf8mb4 default collate utf8mb4_unicode_ci" true 'ALTER TABLE `ddl_reentrant`.`t3` CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI'
	"alter schema ddl_reentrant default character set utf8mb4 default collate utf8mb4_unicode_ci" true 'ALTER DATABASE `ddl_reentrant` CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci'
)

function complete_ddls() {
	# TODO: refine the release detection after 5.0 tag of TiDB is ready
	if [[ ! $tidb_build_branch =~ master ]]; then
		echo "skip some DDLs in tidb v4.0.x"
	else
		# DDLs that are supportted since 5.0
		ddls+=("alter table ddl_reentrant.t2 add column c1 int, add column c2 int, add column c3 int" false 'ALTER TABLE `ddl_reentrant`.`t2` ADD COLUMN `c1` INT, ADD COLUMN `c2` INT, ADD COLUMN `c3` INT')
		ddls+=("alter table ddl_reentrant.t2 drop column c1, drop column c2, drop column c3" false 'ALTER TABLE `ddl_reentrant`.`t2` DROP COLUMN `c1`, DROP COLUMN `c2`, DROP COLUMN `c3`')
		ddls+=("rename table ddl_reentrant.t2 to ddl_reentrant.tt2, ddl_reentrant.t3 to ddl_reentrant.tt3" false 'RENAME TABLE `ddl_reentrant`.`t2` TO `ddl_reentrant`.`tt2`, `ddl_reentrant`.`t3` TO `ddl_reentrant`.`tt3`')
		ddls+=("rename table ddl_reentrant.tt2 to ddl_reentrant.t2, ddl_reentrant.tt3 to ddl_reentrant.t3" false 'RENAME TABLE `ddl_reentrant`.`tt2` TO `ddl_reentrant`.`t2`, `ddl_reentrant`.`tt3` TO `ddl_reentrant`.`t3`')
	fi
	ddls+=("alter table ddl_reentrant.t2 drop primary key" false 'ALTER TABLE `ddl_reentrant`.`t2` DROP PRIMARY KEY')
	ddls+=("alter table ddl_reentrant.t2 add primary key pk(id)" false 'ALTER TABLE `ddl_reentrant`.`t2` ADD PRIMARY KEY `pk`(`id`)')
	ddls+=("drop table ddl_reentrant.t2" false 'DROP TABLE `ddl_reentrant`.`t2`')
	ddls+=("recover table ddl_reentrant.t2" false 'RECOVER TABLE `ddl_reentrant`.`t2`')
	ddls+=("drop database ddl_reentrant" false 'DROP DATABASE `ddl_reentrant`')
}

changefeedid=""
# this test contains `recover table`, which requires super privilege, so we
# can't use the normal user
SINK_URI="mysql://root@127.0.0.1:3306/"

function check_ts_forward() {
	changefeedid=$1
	rts1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.status."resolved-ts"')
	checkpoint1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.status."checkpoint-ts"')
	sleep 1
	rts2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.status."resolved-ts"')
	checkpoint2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1 | jq '.status."checkpoint-ts"')
	if [[ "$rts1" != "null" ]] && [[ "$rts1" != "0" ]]; then
		if [[ "$rts1" -ne "$rts2" ]] || [[ "$checkpoint1" -ne "$checkpoint2" ]]; then
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
	log=$(grep "${key_word}" ${log_file} | tail -n 1)
	if [[ $log == *"${ddl}"* ]]; then
		echo $log
		return
	else
		exit 1
	fi
}

export -f check_ts_forward
export -f check_ddl_executed

tidb_build_branch=$(mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -e \
	"select tidb_version()\G" | grep "Git Branch" | awk -F: '{print $(NF)}' | tr -d " ")

function ddl_test() {
	ddl=$1
	is_reentrant=$2
	restored_sql=$3

	echo "------------------------------------------"
	echo "test ddl $ddl, is_reentrant: $is_reentrant restored_sql: $restored_sql"

	run_sql $ddl ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 10 check_ts_forward $changefeedid

	echo $restored_sql >${WORK_DIR}/ddl_temp.sql
	ensure 10 check_ddl_executed "${WORK_DIR}/cdc.log" "${WORK_DIR}/ddl_temp.sql" true
	ddl_finished_ts=$(grep "Execute DDL succeeded" ${WORK_DIR}/cdc.log | tail -n 1 | grep -oE '"CommitTs\\":[0-9]{18}' | awk -F: '{print $(NF)}')
	cdc cli changefeed remove --changefeed-id=${changefeedid}
	changefeedid=$(cdc cli changefeed create --no-confirm --start-ts=${ddl_finished_ts} --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	echo "create new changefeed ${changefeedid} from ${ddl_finished_ts}"
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
	# TODO: refine the release detection after 5.0 tag of TiDB is ready
	if [[ $tidb_build_branch =~ master ]]; then
		# https://github.com/pingcap/tidb/pull/21533 disables multi_schema change
		# feature by default, turn it on first
		run_sql "set global tidb_enable_change_multi_schema = on" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		# This must be set before cdc server starts
		run_sql "set global tidb_enable_change_multi_schema = on" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
		# TiDB global variables cache 2 seconds at most
		sleep 2
	fi

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	changefeedid=$(cdc cli changefeed create --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	OLDIFS=$IFS
	IFS=""
	idx=0
	while [ $idx -lt ${#ddls[*]} ]; do
		ddl=${ddls[$idx]}
		idx=$((idx + 1))
		idxs_reentrant=${ddls[$idx]}
		idx=$((idx + 1))
		restored_sql=${ddls[$idx]}
		idx=$((idx + 1))
		ddl_test $ddl $idxs_reentrant $restored_sql
	done
	IFS=$OLDIFS

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
