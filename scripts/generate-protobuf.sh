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

TOOLS_BIN_DIR="tools/bin"
TOOLS_INCLUDE_DIR="tools/include"
PROTO_DIR="proto"
DM_PROTO_DIR="dm/proto"
TiCDC_SOURCE_DIR="cdc"
INCLUDE="-I $PROTO_DIR \
	-I $TOOLS_INCLUDE_DIR \
	-I $TiCDC_SOURCE_DIR \
	-I $DM_PROTO_DIR"

PROTOC="$TOOLS_BIN_DIR/protoc"
GO="$TOOLS_BIN_DIR/protoc-gen-go"
GO_GRPC="$TOOLS_BIN_DIR/protoc-gen-go-grpc"
GOGO_FASTER=$TOOLS_BIN_DIR/protoc-gen-gogofaster
GRPC_GATEWAY=$TOOLS_BIN_DIR/protoc-gen-grpc-gateway
GRPC_GATEWAY_V2=$TOOLS_BIN_DIR/protoc-gen-grpc-gateway-v2
OPENAPIV2=tools/bin/protoc-gen-openapiv2

function generate() {
	local out_dir=$1
	local proto_file=$2
	local gogo_option=
	if [ $# -eq 3 ]; then
		gogo_option=$3
	fi

	echo "generate $proto_file..."
	$PROTOC $INCLUDE \
		--plugin=protoc-gen-gogofaster="$GOGO_FASTER" \
		--gogofaster_out=$gogo_option:$out_dir $proto_file
}

for tool in $PROTOC $GO $GO_GRPC $GOGO_FASTER $GRPC_GATEWAY $GRPC_GATEWAY_V2 $OPENAPIV2; do
	if [ ! -x $tool ]; then
		echo "$tool does not exist, please run 'make $tool' first."
		exit 1
	fi
done

generate ./proto/canal ./proto/EntryProtocol.proto
generate ./proto/canal ./proto/CanalProtocol.proto
generate ./proto/benchmark ./proto/CraftBenchmark.proto
generate ./proto/p2p ./proto/CDCPeerToPeer.proto plugins=grpc
generate ./dm/pb ./dm/proto/dmworker.proto plugins=grpc,protoc-gen-grpc-gateway="$GRPC_GATEWAY"
generate ./dm/pb ./dm/proto/dmmaster.proto plugins=grpc,protoc-gen-grpc-gateway="$GRPC_GATEWAY"

for pb in $(find cdc -name '*.proto'); do
	# Output generated go files next to protobuf files.
	generate ./cdc $pb paths="source_relative"
done

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
