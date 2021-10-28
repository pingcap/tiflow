#!/usr/bin/env bash

source ./tests/util.sh

check_db_status "${MYSQL_HOST:-127.0.0.1}" "${MYSQL_PORT:-3306}" mysql
