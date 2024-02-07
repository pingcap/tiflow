#!/bin/bash

set -eux

export TEST_DIR=/tmp/dm_test
export TEST_NAME="upgrade-via-tiup"

WORK_DIR=$TEST_DIR/$TEST_NAME
mkdir -p $WORK_DIR

TASK_NAME="upgrade_via_tiup"
TASK_PESS_NAME="upgrade_via_tiup_pessimistic"
TASK_OPTI_NAME="upgrade_via_tiup_optimistic"

DB1=Sharding1
DB2=sharding2
# can't run upgrade test with upper case schema name in optimist mode
DB3=opt_sharding1
DB4=opt_sharding2
DB5=pes_Sharding1
DB6=pes_sharding2
TBL1=T1
TBL2=t2
TBL3=T3
TBL4=t4
TBL5=t5
TBL_LOWER1=t1
TBL_LOWER3=t3
PRE_VER=${PRE_VER:-v1.0.7}

function exec_sql() {
	echo $3 | mysql -h $1 -P $2
}

function run_sql_tidb_with_retry() {
	rc=0
	for ((k = 1; k < 11; k++)); do
		# in retry scenario sometimes run_sql_tidb will fail because "table not exist", we should keep retrying so turn
		# off the error option temporarily.
		set +e
		echo $1 | mysql -h tidb -P 4000 >"/tmp/res.txt"
		set -e
		if grep -Fq "$2" "/tmp/res.txt"; then
			rc=1
			break
		fi
		echo "run tidb sql failed $k-th time, retry later"
		sleep 2
	done
	if [[ $rc = 0 ]]; then
		echo "TEST FAILED: OUTPUT DOES NOT CONTAIN '$2'"
		echo "____________________________________"
		cat "/tmp/res.txt"
		echo "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
		exit 1
	fi
}

function install_sync_diff() {
	curl https://download.pingcap.org/tidb-enterprise-tools-nightly-linux-amd64.tar.gz | tar xz
	mkdir -p bin
	mv tidb-enterprise-tools-nightly-linux-amd64/bin/sync_diff_inspector bin/
}

function exec_full_stage() {
	# drop previous data
	exec_sql mysql1 3306 "DROP DATABASE IF EXISTS $DB1;"
	exec_sql mariadb2 3306 "DROP DATABASE IF EXISTS $DB2;"
	exec_sql mysql1 3306 "DROP DATABASE IF EXISTS $DB3;"
	exec_sql mariadb2 3306 "DROP DATABASE IF EXISTS $DB4;"
	exec_sql mysql1 3306 "DROP DATABASE IF EXISTS $DB5;"
	exec_sql mariadb2 3306 "DROP DATABASE IF EXISTS $DB6;"
	exec_sql tidb 4000 "DROP DATABASE IF EXISTS db_target;"
	exec_sql tidb 4000 "DROP DATABASE IF EXISTS opt_db_target;"
	exec_sql tidb 4000 "DROP DATABASE IF EXISTS pes_db_target;"
	exec_sql tidb 4000 "DROP DATABASE IF EXISTS dm_meta;"

	# # prepare full data
	exec_sql mysql1 3306 "CREATE DATABASE $DB1;"
	exec_sql mariadb2 3306 "CREATE DATABASE $DB2;"
	exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL1 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB2.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB2.$TBL3 (c1 INT PRIMARY KEY, c2 TEXT);"

	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (1, '1');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (2, '2');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (11, '11');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (12, '12');"

	# prepare optimsitic full data
	exec_sql mysql1 3306 "CREATE DATABASE $DB3 CHARACTER SET UTF8MB4 COLLATE utf8mb4_bin;"
	exec_sql mariadb2 3306 "CREATE DATABASE $DB4 CHARACTER SET UTF8MB4 COLLATE utf8mb4_bin;"
	exec_sql mysql1 3306 "CREATE TABLE $DB3.$TBL_LOWER1 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mysql1 3306 "CREATE TABLE $DB3.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB4.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB4.$TBL_LOWER3 (c1 INT PRIMARY KEY, c2 TEXT);"

	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL_LOWER1 (c1, c2) VALUES (1, '1');"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2) VALUES (2, '2');"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2) VALUES (11, '11');"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL_LOWER3 (c1, c2) VALUES (12, '12');"

	# prepare pessimistic full data
	exec_sql mysql1 3306 "CREATE DATABASE $DB5;"
	exec_sql mariadb2 3306 "CREATE DATABASE $DB6;"
	exec_sql mysql1 3306 "CREATE TABLE $DB5.$TBL1 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mysql1 3306 "CREATE TABLE $DB5.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB6.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB6.$TBL3 (c1 INT PRIMARY KEY, c2 TEXT);"

	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL1 (c1, c2) VALUES (1, '1');"
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL2 (c1, c2) VALUES (2, '2');"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL2 (c1, c2) VALUES (11, '11');"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL3 (c1, c2) VALUES (12, '12');"
}

function exec_incremental_stage1() {
	# prepare incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (101, '101');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (102, '102');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (111, '111');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (112, '112');"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL_LOWER1 (c1, c2) VALUES (101, '101');"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2) VALUES (102, '102');"
	# v2.0.0 has bug with create/drop table
	if [[ "$PRE_VER" != "v2.0.0" ]]; then
		exec_sql mysql1 3306 "CREATE TABLE $DB3.$TBL5(c1 INT PRIMARY KEY, c2 TEXT);"
		exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL5 (c1, c2) VALUES (1000, 1000);"
	fi
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2) VALUES (111, '111');"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL_LOWER3 (c1, c2) VALUES (112, '112');"

	if [[ "$PRE_VER" != "v2.0.0" ]]; then
		exec_sql mariadb2 3306 "CREATE TABLE $DB4.$TBL4(c1 INT PRIMARY KEY, c2 TEXT);"
		exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL4 (c1, c2) VALUES (115, '115');"
		exec_sql mysql1 3306 "DROP TABLE $DB3.$TBL5;"
	fi

	# optimistic shard ddls
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL_LOWER1 ADD COLUMN c3 INT;"
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL2 ADD COLUMN c4 INT;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL2 ADD COLUMN c3 INT;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL_LOWER3 ADD COLUMN c4 INT;"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL_LOWER1 (c1, c2, c3) VALUES (103, '103', 103);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c4) VALUES (104, '104', 104);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3) VALUES (113, '113', 113);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL_LOWER3 (c1, c2, c4) VALUES (114, '114', 114);"
	if [[ "$PRE_VER" != "v2.0.0" ]]; then
		exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL4 (c1, c2) VALUES (116, '116');"
	fi

	# prepare pessimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL1 (c1, c2) VALUES (101, '101');"
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL2 (c1, c2) VALUES (102, '102');"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL2 (c1, c2) VALUES (111, '111');"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL3 (c1, c2) VALUES (112, '112');"

	# pessimistic shard ddls
	exec_sql mysql1 3306 "ALTER TABLE $DB5.$TBL1 ADD COLUMN c3 INT;"
	exec_sql mysql1 3306 "ALTER TABLE $DB5.$TBL2 ADD COLUMN c3 INT;"

	# prepare pessimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL1 (c1, c2, c3) VALUES (103, '103', 103);"
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL2 (c1, c2, c3) VALUES (104, '104', 104);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL2 (c1, c2) VALUES (113, '113');"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL3 (c1, c2) VALUES (114, '114');"
}

function exec_incremental_stage2() {
	# prepare incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (201, '201');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (202, '202');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (211, '211');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (212, '212');"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL_LOWER1 (c1, c2, c3) VALUES (201, '201', 201);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c4) VALUES (202, '202', 202);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3) VALUES (211, '211', 211);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL_LOWER3 (c1, c2, c4) VALUES (212, '212', 212);"

	# optimistic shard ddls
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL_LOWER1 ADD COLUMN c4 INT;"
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL2 ADD COLUMN c3 INT AFTER c2;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL2 ADD COLUMN c4 INT;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL_LOWER3 ADD COLUMN c3 INT AFTER c2;"

	if [[ "$PRE_VER" != "v2.0.0" ]]; then
		exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL4 ADD COLUMN c4 INT;"
		exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL4 ADD COLUMN c3 INT AFTER c2;"
	fi

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL_LOWER1 (c1, c2, c3, c4) VALUES (203, '203', 203, 203);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c3, c4) VALUES (204, '204', 204, 204);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3, c4) VALUES (213, '213', 213, 213);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL_LOWER3 (c1, c2, c3, c4) VALUES (214, '214', 214, 214);"
	if [[ "$PRE_VER" != "v2.0.0" ]]; then
		exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL4 (c1, c2, c3, c4) VALUES (215, '215', 215, 215);"
	fi

	# prepare pessimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL1 (c1, c2, c3) VALUES (201, '201', 201);"
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL2 (c1, c2, c3) VALUES (202, '202', 202);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL2 (c1, c2) VALUES (211, '211');"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL3 (c1, c2) VALUES (212, '212');"

	# pessimistic shard ddls
	exec_sql mariadb2 3306 "ALTER TABLE $DB6.$TBL2 ADD COLUMN c3 INT;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB6.$TBL3 ADD COLUMN c3 INT;"

	# prepare pessimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL1 (c1, c2, c3) VALUES (203, '203', 203);"
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL2 (c1, c2, c3) VALUES (204, '204', 204);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL2 (c1, c2, c3) VALUES (213, '213', 213);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL3 (c1, c2, c3) VALUES (214, '214', 214);"
}

function exec_incremental_stage3() {
	# prepare incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (301, '301');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (302, '302');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (311, '311');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (312, '312');"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL_LOWER1 (c1, c2, c3, c4) VALUES (301, '301', 301, 301);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c3, c4) VALUES (302, '302', 302, 302);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3, c4) VALUES (311, '311', 311, 311);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL_LOWER3 (c1, c2, c3, c4) VALUES (312, '312', 312, 312);"
	if [[ "$PRE_VER" != "v2.0.0" ]]; then
		exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL4 (c1, c2, c3, c4) VALUES (313, '313', 313, 313);"
	fi

	# prepare pessimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL1 (c1, c2, c3) VALUES (303, '303', 303);"
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL2 (c1, c2, c3) VALUES (304, '304', 304);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL2 (c1, c2, c3) VALUES (313, '313', 313);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL3 (c1, c2, c3) VALUES (314, '314', 314);"
}

function exec_incremental_stage4() {
	# prepare incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (401, '401');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (402, '402');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (411, '411');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (412, '412');"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL_LOWER1 (c1, c2, c3, c4) VALUES (401, '401', 401, 401);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c3, c4) VALUES (402, '402', 402, 402);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3, c4) VALUES (411, '411', 411, 411);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL_LOWER3 (c1, c2, c3, c4) VALUES (412, '412', 412, 412);"
	if [[ "$PRE_VER" != "v2.0.0" ]]; then
		exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL4 (c1, c2, c3, c4) VALUES (413, '413', 413, 413);"
	fi

	# prepare pessimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL1 (c1, c2, c3) VALUES (403, '403', 403);"
	exec_sql mysql1 3306 "INSERT INTO $DB5.$TBL2 (c1, c2, c3) VALUES (404, '404', 404);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL2 (c1, c2, c3) VALUES (413, '413', 413);"
	exec_sql mariadb2 3306 "INSERT INTO $DB6.$TBL3 (c1, c2, c3) VALUES (414, '414', 414);"
}

function patch_nightly_with_tiup_mirror() {
	# clone packages for upgrade.
	# FIXME: use nightly version of grafana and prometheus after https://github.com/pingcap/tiup/issues/1334 fixed.
	tiup --version
	tiup mirror clone tidb-dm-nightly-linux-amd64 --os=linux --arch=amd64 \
		--alertmanager=v0.17.0 --grafana=v5.0.1 --prometheus=v5.0.1 \
		--dm-master=$1 --dm-worker=$1 \
		--tiup=v$(tiup --version | head -n1 | awk '{print $1}') --dm=v$(tiup --version | head -n1 | awk '{print $1}')

	# change tiup mirror
	tidb-dm-nightly-linux-amd64/local_install.sh

	# publish nightly version
	# binary files have already been built and packaged.
	tiup mirror genkey
	tiup mirror grant gmhdbjd --name gmhdbjd
	mv tidb-dm-nightly-linux-amd64/keys/*-pingcap.json ./pingcap.json
	tiup mirror publish dm-master nightly /tmp/dm-master-nightly-linux-amd64.tar.gz dm-master/dm-master --arch amd64 --os linux --desc="dm-master component of Data Migration Platform" -k pingcap.json
	tiup mirror publish dm-worker nightly /tmp/dm-worker-nightly-linux-amd64.tar.gz dm-worker/dm-worker --arch amd64 --os linux --desc="dm-worker component of Data Migration Platform" -k pingcap.json
	tiup mirror publish dmctl nightly /tmp/dmctl-nightly-linux-amd64.tar.gz dmctl/dmctl --arch amd64 --os linux --desc="dmctl component of Data Migration Platform"

	tiup list
}

function run_dmctl_with_retry() {
	dmctl_log="dmctl.log"
	for ((k = 0; k < 10; k++)); do
		tiup dmctl:$1 --master-addr=master1:8261 $2 >$dmctl_log 2>&1
		all_matched=true
		for ((i = 3; i < $#; i += 2)); do
			j=$((i + 1))
			value=${!i}
			expected=${!j}
			got=$(sed "s/$value/$value\n/g" $dmctl_log | grep -c "$value" || true)
			if [ "$got" != "$expected" ]; then
				all_matched=false
				break
			fi
		done

		if $all_matched; then
			return 0
		fi

		sleep 2
	done

	cat $dmctl_log
	exit 1
}

function ensure_start_relay() {
	# manually enable relay for source1 after v2.0.2
	if [[ "$PRE_VER" == "v2.0.0" ]] || [[ "$PRE_VER" == "v2.0.1" ]]; then
		return
	fi

	dmctl_log="get-worker.txt"
	# always use CUR_VER, because we might use tiup mirror in previous steps.
	tiup dmctl:$CUR_VER --master-addr=master1:8261 operate-source show -s mysql-replica-01 >$dmctl_log 2>&1
	worker=$(grep "worker" $dmctl_log | awk -F'"' '{ print $4 }')
	if [[ "$PRE_VER" == "v2.0.2" ]] || [[ "$PRE_VER" == "v2.0.3" ]] || [[ "$PRE_VER" == "v2.0.4" ]] || [[ "$PRE_VER" == "v2.0.5" ]] || [[ "$PRE_VER" == "v2.0.6" ]] || [[ "$PRE_VER" == "v2.0.7" ]]; then
		run_dmctl_with_retry $CUR_VER "start-relay -s mysql-replica-01 $worker" "\"result\": true" 1
	else
		run_dmctl_with_retry $CUR_VER "start-relay -s mysql-replica-01" "\"result\": true" 1
	fi
}

function restart_relay() {
	if [[ "$PRE_VER" == "v2.0.0" ]] || [[ "$PRE_VER" == "v2.0.1" ]]; then
		return
	fi

	run_dmctl_with_retry $CUR_VER "stop-relay -s mysql-replica-01" "\"result\": true" 1
	run_dmctl_with_retry $CUR_VER "query-status -s mysql-replica-01" "\"relayStatus\": null" 1
	run_dmctl_with_retry $CUR_VER "start-relay -s mysql-replica-01" "\"result\": true" 1
	run_dmctl_with_retry $CUR_VER "query-status -s mysql-replica-01" "relayCatchUpMaster" 1
}
