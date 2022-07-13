#!/bin/bash
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

# If the environment variable is unset, GOPATH defaults
# to a subdirectory named "go" in the user's home directory
# ($HOME/go on Unix, %USERPROFILE%\go on Windows),
# unless that directory holds a Go distribution.
# Run "go env GOPATH" to see the current GOPATH.
GOPATH=$(go env GOPATH)
echo "using GOPATH=$GOPATH"

cd "$(dirname "${BASH_SOURCE[0]}")"/..
TOOLS_BIN_DIR=$(pwd)/tools/bin

case "$(uname)" in
MINGW*)
	EXE=.exe
	;;
*)
	EXE=
	;;
esac

# use `protoc-gen-gogofaster` rather than `protoc-gen-go`.
GOGO_FASTER=$TOOLS_BIN_DIR/protoc-gen-gogofaster$EXE
if [ ! -f "${GOGO_FASTER}" ]; then
	echo "${GOGO_FASTER} does not exist, please run 'make tools_setup' first"
	exit 1
fi

# get and construct the path to gogo/protobuf.
GO111MODULE=on go get -d github.com/gogo/protobuf
GOGO_MOD=$(GO111MODULE=on go list -m github.com/gogo/protobuf)
GOGO_PATH=$GOPATH/pkg/mod/${GOGO_MOD// /@}
if [ ! -d "${GOGO_PATH}" ]; then
	echo "${GOGO_PATH} does not exist, please ensure 'github.com/gogo/protobuf' is in go.mod"
	exit 1
fi

# we need `grpc-gateway` to generate HTTP API from gRPC API.
#GRPC_GATEWAY=$TOOLS_BIN_DIR/protoc-gen-grpc-gateway$EXE
#if [ ! -f ${GRPC_GATEWAY} ]; then
#    echo "${GRPC_GATEWAY} does not exist, please run 'make tools_setup' first"
#    exit 1
#fi

# fetch `github.com/googleapis/googleapis`, seems no `.go` file in it,
# so neither `go.mod` nor `retool` can track it.
# hardcode the version here now.
#GO111MODULE=on go get github.com/googleapis/googleapis@91ef2d9
#GAPI_MOD=$(GO111MODULE=on go list -m github.com/googleapis/googleapis)
#GAPI_PATH=$GOPATH/pkg/mod/${GAPI_MOD// /@}
#GO111MODULE=on go mod tidy  # keep `go.mod` and `go.sum` tidy
#if [ ! -d ${GAPI_PATH} ]; then
#    echo "${GAPI_PATH} does not exist, try verify it manually"
#    exit 1
#fi

echo "generate protobuf code..."

#cp -r ${GAPI_PATH}/google ./

"${TOOLS_BIN_DIR}/protoc" \
	-Iengine/proto/ -I"${GOGO_PATH}" -I"${GOGO_PATH}/protobuf" \
	--plugin=protoc-gen-gogofaster="${GOGO_FASTER}" \
	--gogofaster_out=plugins=grpc:engine/enginepb/ \
	engine/proto/*.proto

#chmod -R +w ./google  # permission is `-r--r--r--`
#rm -r ./google

sed -i.bak -E 's/import _ \"gogoproto\"//g' engine/enginepb/*.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' engine/enginepb/*.pb.go
rm -f engine/enginepb/*.bak
"${TOOLS_BIN_DIR}/goimports" -w engine/enginepb/*.pb.go
