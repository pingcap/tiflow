#!/bin/bash

set -eu

db="handle_error"
tb1="tb1"
tb2="tb2"
tb="tb"
ta1="ta1"
ta2="ta2"
ta="ta"
source1="mysql-replica-01"
source2="mysql-replica-02"

function init_database() {
	run_sql_both_source "drop database if exists ${db};"
	run_sql_both_source "create database if not exists ${db};"
}

function extract() {
	str="$1"
	source=${str:0:1}
	table=${str:1:1}
}

# set charset explicitly because MySQL 8.0 has different default charsets.
function init_table() {
	for i in $@; do
		extract $i
		run_sql_source${source} "create table ${db}.tb${table} (id int primary key) CHARSET=latin1 COLLATE=latin1_bin;"
	done
}

function clean_table() {
	run_sql_both_source "drop table if exists ${db}.${tb1};"
	run_sql_both_source "drop table if exists ${db}.${tb2};"
	run_sql_both_source "drop table if exists ${db}.${ta1};"
	run_sql_both_source "drop table if exists ${db}.${ta2};"
	run_sql_tidb "drop table if exists ${db}.${tb};"
	run_sql_tidb "drop table if exists ${db}.${ta};"
	run_sql_tidb "drop table if exists ${db}.${tb1};"
	run_sql_tidb "drop table if exists ${db}.${tb2};"
	run_sql_tidb "drop database if exists dm_meta;"
}

function get_start_location() {
	location=$($PWD/bin/dmctl.test DEVEL --master-addr=$1 \
		query-status test -s $2 |
		grep Location |
		gawk 'match($0,/startLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\], endLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\]/,a) {printf "%s:%s", a[1], a[2]}')
	echo ${location}
}

function get_end_location() {
	location=$($PWD/bin/dmctl.test DEVEL --master-addr=$1 \
		query-status test -s $2 |
		grep Location |
		gawk 'match($0,/startLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\], endLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\]/,a) {printf "%s:%s", a[4], a[5]}')
	echo ${location}
}

function get_start_pos() {
	pos=$($PWD/bin/dmctl.test DEVEL --master-addr=$1 \
		query-status test -s $2 |
		grep Location |
		gawk 'match($0,/startLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\], endLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\]/,a) {print a[2]}')
	echo ${pos}
}

function get_start_name() {
	pos=$($PWD/bin/dmctl.test DEVEL --master-addr=$1 \
		query-status test -s $2 |
		grep Location |
		gawk 'match($0,/startLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\], endLocation: \[position: \((.*), (.*)\), gtid-set: (.*)\]/,a) {print a[1]}')
	echo ${pos}
}

# get next ddl position base on $3
function get_next_query_pos() {
	pos=$(mysql -uroot -P$1 -h127.0.0.1 -p$2 -e 'show binlog events' | grep Query | awk '{ if ($2>'"$3"' && $6!="BEGIN") {print $2; exit} }')
	echo ${pos}
}
