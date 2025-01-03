#!/bin/bash

TEST_DATABASE_NAME=checker_test
IMPORT_EXEC="../bin/importer -c 1 -h ${MYSQL_HOST} -P ${MYSQL_PORT} -D ${TEST_DATABASE_NAME}"
MYSQL_EXEC="mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u root"

init(){
    check_db_status "${MYSQL_HOST}" "${MYSQL_PORT}" mysql "."
    ${MYSQL_EXEC} -e "drop database if exists ${TEST_DATABASE_NAME};"
    ${MYSQL_EXEC} -e "create database ${TEST_DATABASE_NAME};"
}

destroy(){
    ${MYSQL_EXEC} -e "drop database if exists ${TEST_DATABASE_NAME};"
}

testImporter(){
    ${IMPORT_EXEC} -c 1 -n 10 -t "$1" -i "$2"
    RESULT=`${MYSQL_EXEC} -e "$3" | sed -n '2p'`
    if [[ "${RESULT}" != "$4" ]]; then
        echo "Test importer failed: $1"
        exit 1
    fi
}

set -e
init
testImporter "create table ta(a int primary key, b double, c varchar(10), d date unique, e time unique, f timestamp unique, g date unique, h datetime unique, i year unique);" "create unique index u_b on ta(b);" "select count(*) as result from ${TEST_DATABASE_NAME}.ta" "10"
testImporter "create table tb(a int comment '[[range=1,10]]');" "" "select count(*) as result from ${TEST_DATABASE_NAME}.tb where a <= 10 and a >= 1" "10"
testImporter "create table tc(a int unique comment '[[step=2]]');" "" "select sum(a) as result from ${TEST_DATABASE_NAME}.tc" "90"
testImporter "create table td(a int comment '[[set=1,2,3]]');" "" "select count(*) as result from ${TEST_DATABASE_NAME}.td where a <= 3 and a >= 1" "10"
destroy
