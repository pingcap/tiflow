#!/usr/bin/env bash

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source ${CUR}/util.sh

check_db_status "${MYSQL_HOST:-127.0.0.1}" "${MYSQL_PORT:-3306}" mysql
