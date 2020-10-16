#!/bin/sh

# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a script designed for testing canal support for Kafka sink.
# The data flow is (upstream TiDB) ==> (TiCDC) =canal=> (kafka) =canal=> (canal consumer/adapter) =sql=> (downstream TiDB)
# Developers should run "canal-local-test.sh up" first, wait for a while, run "canal-local-test.sh init", and only
# then will the data flow be established.
#
# Upstream mysql://127.0.0.1:4000
# Downstream mysql://127.0.0.1:5000
#
# This script only supports syncing (dbname = testdb).
# Developers should adapt this file to their own needs.
#

ProgName=$(basename $0)
UPSTREAM_DB_HOST=127.0.0.1
UPSTREAM_DB_PORT=4000
DOWNSTREAM_DB_HOST=127.0.0.1
DOWNSTREAM_DB_PORT=5000
DB_NAME=testdb
TOPIC_NAME=${DB_NAME}
cd "$(dirname "$0")"

prepare_db() {
  echo "Verifying Upstream TiDB is started..."
  i=0
  while ! mysql -uroot -h${UPSTREAM_DB_HOST} -P${UPSTREAM_DB_PORT}  -e 'select * from mysql.tidb;'; do
      i=$((i + 1))
      if [ "$i" -gt 60 ]; then
          echo 'Failed to start upstream TiDB'
          exit 2
      fi
      sleep 2
  done

  echo "Verifying Downstream TiDB is started..."
  i=0
  while ! mysql -uroot -h${DOWNSTREAM_DB_HOST} -P${DOWNSTREAM_DB_PORT}  -e 'select * from mysql.tidb;'; do
      i=$((i + 1))
      if [ "$i" -gt 60 ]; then
          echo 'Failed to start downstream TiDB'
          exit 1
      fi
      sleep 2
  done
  sql="drop database if exists ${DB_NAME}; create database ${DB_NAME};"
  echo "[$(date)] Executing SQL: ${sql}"
  mysql -uroot -h ${UPSTREAM_DB_HOST} -P ${UPSTREAM_DB_PORT}  -E -e "${sql}"
  mysql -uroot -h ${DOWNSTREAM_DB_HOST} -P ${DOWNSTREAM_DB_PORT}  -E -e "${sql}"
}


sub_help() {
    echo "Usage: $ProgName <subcommand> [options]\n"
    echo "Subcommands:"
    echo "    init    Prepare database and Create changefeed"
    echo "    up      Bring up the containers"
    echo "    down    Pause the containers"
    echo ""
}

sub_init() {
  prepare_db
  sudo docker exec -it ticdc_controller_1 sh -c "
  /cdc cli changefeed create --pd=\"http://upstream-pd:2379\" --sink-uri=\"kafka://kafka:9092/testdb\" --config=\"/config/canal-test-config.toml\" --opts \"force-handle-key-pkey=true, support-txn=true\"
  "
}

sub_up() {
  sudo docker-compose -f ../../docker-compose-canal.yml up -d
}

sub_down() {
  sudo docker-compose -f ../../docker-compose-canal.yml down
  sudo rm -r ../../docker/logs ../../docker/data
}

subcommand=$1
case $subcommand in
    "" | "-h" | "--help")
        sub_help
        ;;
    *)
        shift
        sub_${subcommand} '$@'
        if [ $? = 127 ]; then
            echo "Error: '$subcommand' is not a known subcommand." >&2
            echo "       Run '$ProgName --help' for a list of known subcommands." >&2
            exit 1
        fi
        ;;
esac
