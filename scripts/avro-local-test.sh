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

# This is a script designed for testing Avro support for Kafka sink.
# The data flow is (upstream TiDB) ==> (TiCDC) =avro=> (Kafka/JDBC connector) =sql=> (downstream TiDB)
# Developers should run "avro-local-test.sh up" first, wait for a while, run "avro-local-test.sh init", and only
# then will the data flow be established.
#
# Upstream mysql://127.0.0.1:4000
# Downstream mysql://127.0.0.1:5000
#
# This script only supports syncing (dbname = testdb, tableName = test).
# Developers should adapt this file to their own needs.
#

ProgName=$(basename $0)
cd "$(dirname "$0")"
sub_help() {
	echo "Usage: $ProgName <subcommand> [options]\n"
	echo "Subcommands:"
	echo "    init    Create changefeed and Kafka Connector"
	echo "    up      Bring up the containers"
	echo "    down    Pause the containers"
	echo ""
}

sub_init() {
	sudo docker exec -it ticdc_controller_1 sh -c "
  /cdc cli changefeed create --pd=\"http://upstream-pd:2379\" --sink-uri=\"kafka://kafka:9092/testdb_test?protocol=avro\"
  curl -X POST -H \"Content-Type: application/json\" -d @/config/jdbc-sink-connector.json http://kafka-connect-01:8083/connectors
  "
}

sub_up() {
	sudo docker-compose -f ../deployments/ticdc/docker-compose/docker-compose-avro.yml up --detach
}

sub_down() {
	sudo docker-compose -f ../deployments/ticdc/docker-compose/docker-compose-avro.yml down
	sudo rm -r ../deployments/ticdc/docker-compose/logs ../deployments/ticdc/docker-compose/data
}

subcommand=$1
case $subcommand in
"" | "-h" | "--help")
	sub_help
	;;
*)
	shift
	sub_${subcommand} $@
	if [ $? = 127 ]; then
		echo "Error: '$subcommand' is not a known subcommand." >&2
		echo "       Run '$ProgName --help' for a list of known subcommands." >&2
		exit 1
	fi
	;;
esac
