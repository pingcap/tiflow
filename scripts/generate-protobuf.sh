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

PROTOC="$TOOLS_BIN_DIR/protoc"
GO="$TOOLS_BIN_DIR/protoc-gen-go"
GO_GRPC="$TOOLS_BIN_DIR/protoc-gen-go-grpc"
GOGO_FASTER=$TOOLS_BIN_DIR/protoc-gen-gogofaster
GRPC_GATEWAY=$TOOLS_BIN_DIR/protoc-gen-grpc-gateway
GRPC_GATEWAY_V2=$TOOLS_BIN_DIR/protoc-gen-grpc-gateway-v2
OPENAPIV2=tools/bin/protoc-gen-openapiv2

for tool in $PROTOC $GO $GO_GRPC $GOGO_FASTER $GRPC_GATEWAY $GRPC_GATEWAY_V2 $OPENAPIV2; do
	if [ ! -x $tool ]; then
		echo "$tool does not exist, please run 'make $tool' first."
		exit 1
	fi
done

echo "generate canal..."
mkdir -p ./proto/canal
$PROTOC -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=./proto/canal ./proto/EntryProtocol.proto
$PROTOC -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=./proto/canal ./proto/CanalProtocol.proto

echo "generate craft benchmark protocol..."
mkdir -p ./proto/benchmark
$PROTOC -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=./proto/benchmark ./proto/CraftBenchmark.proto

echo "generate p2p..."
mkdir -p ./proto/p2p
$PROTOC -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=plugins=grpc:./proto/p2p ./proto/CDCPeerToPeer.proto

echo "generate schedulepb..."
mkdir -p ./cdc/scheduler/internal/v3/schedulepb
$PROTOC -I"./proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=plugins=grpc:./cdc/scheduler/internal/v3/schedulepb ./proto/table_schedule.proto

echo "generate dmpb..."
mkdir -p ./dm/pb
$PROTOC -I"./dm/proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
	--gogofaster_out=plugins=grpc:./dm/pb ./dm/proto/*.proto
$PROTOC -I"./dm/proto" -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-grpc-gateway="$GRPC_GATEWAY" \
	--grpc-gateway_out=./dm/pb ./dm/proto/dmmaster.proto

echo "generate enginepb..."
mkdir -p ./engine/enginepb
$PROTOC -I. -I"$TOOLS_INCLUDE_DIR" \
	--plugin=protoc-gen-go="$GO" \
	--plugin=protoc-gen-go-grpc="$GO_GRPC" \
	--plugin=protoc-gen-grpc-gateway-v2="$GRPC_GATEWAY_V2" \
	--plugin=protoc-gen-openapiv2="$OPENAPIV2" \
	--go_out=. --go_opt=module=github.com/pingcap/tiflow \
	--go-grpc_out=. --go-grpc_opt=module=github.com/pingcap/tiflow,require_unimplemented_servers=false \
	--grpc-gateway-v2_out=. --grpc-gateway-v2_opt=module=github.com/pingcap/tiflow \
	--openapiv2_out=engine/pkg/openapi \
	--openapiv2_opt=allow_merge=true,merge_file_name="apiv1",omit_enum_default_value=true,json_names_for_fields=false \
	engine/proto/*.proto
