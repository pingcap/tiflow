#!/usr/bin/env bash
# Copyright 2022 PingCAP, Inc.
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

set -eu

TOOLS_BIN_DIR=tools/bin
TOOLS_INCLUDE_DIR=tools/include

if [ ! -f "$TOOLS_BIN_DIR/protoc" ]; then
	echo "$TOOLS_BIN_DIR/protoc does not exist, please run 'make tools/bin/protoc' first"
	exit 1
fi

# use `protoc-gen-gogofaster` rather than `protoc-gen-go`.
echo "check gogo..."
GOGO_FASTER=$TOOLS_BIN_DIR/protoc-gen-gogofaster
if [ ! -f "$GOGO_FASTER" ]; then
	echo "${GOGO_FASTER} does not exist, please run 'make tools/bin/protoc-gen-gogofaster' first"
	exit 1
fi

echo "generate canal..."
[ ! -d ./proto/canal ] && mkdir ./proto/canal
$TOOLS_BIN_DIR/protoc -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=./proto/canal ./proto/EntryProtocol.proto
$TOOLS_BIN_DIR/protoc -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=./proto/canal ./proto/CanalProtocol.proto

echo "generate craft benchmark protocol..."
[ ! -d ./proto/benchmark ] && mkdir ./proto/benchmark
$TOOLS_BIN_DIR/protoc -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=./proto/benchmark ./proto/CraftBenchmark.proto

echo "generate p2p..."
[ ! -d ./proto/p2p ] && mkdir ./proto/p2p
$TOOLS_BIN_DIR/protoc -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=plugins=grpc:./proto/p2p ./proto/CDCPeerToPeer.proto
