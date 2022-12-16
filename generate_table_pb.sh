#!/bin/bash

pb="cdc/processor/tablepb/table.proto"
TOOLS_BIN_DIR="tools/bin"
TOOLS_INCLUDE_DIR="tools/include"
PROTO_DIR="proto"
TiCDC_SOURCE_DIR="cdc"
INCLUDE="-I $PROTO_DIR -I $TOOLS_INCLUDE_DIR -I $TiCDC_SOURCE_DIR"
PROTOC="$TOOLS_BIN_DIR/protoc"
GO="$TOOLS_BIN_DIR/protoc-gen-go"
GO_GRPC="$TOOLS_BIN_DIR/protoc-gen-go-grpc"
GO_VT_GRPC="$TOOLS_BIN_DIR/protoc-gen-go-vtproto"

# $PROTOC $INCLUDE --go_out=. --go-grpc_out=. --go-grpc_opt=paths=source_relative --go_opt=paths=source_relative $pb

# $PROTOC $INCLUDE \
#     --go_out=cdc --go_opt=paths=source_relative \
#     --go-grpc_out=cdc --go-grpc_opt=paths=source_relative \
# 	--plugin=protoc-gen-go="$GO" \
# 	--plugin=protoc-gen-go-grpc="$GO_GRPC" \
#     $pb

proto_files=( "cdc/processor/tablepb/table.proto" "cdc/scheduler/schedulepb/table_schedule.proto" )
for pb_file in ${proto_files[@]}; do
    $PROTOC $INCLUDE \
        --go_out=cdc --go_opt=paths=source_relative \
        --plugin=protoc-gen-go="$GO" \
        --go-grpc_out=cdc --plugin=protoc-gen-go-grpc="$GO_GRPC" \
        --go-vtproto_out=. --plugin=protoc-gen-go-vtproto="$GO_VT_GRPC" \
        --go-vtproto_opt=features=marshal+unmarshal+size \
        $pb_file
done
