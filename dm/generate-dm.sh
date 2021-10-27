#!/usr/bin/env bash

# If the environment variable is unset, GOPATH defaults
# to a subdirectory named "go" in the user's home directory
# ($HOME/go on Unix, %USERPROFILE%\go on Windows),
# unless that directory holds a Go distribution.
# Run "go env GOPATH" to see the current GOPATH.
GOPATH=$(go env GOPATH)
echo "using GOPATH=$GOPATH"

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
if [ ! -f ${GOGO_FASTER} ]; then
    echo "${GOGO_FASTER} does not exist, please run 'make tools_setup' first"
    exit 1
fi

# get and construct the path to gogo/protobuf.
GO111MODULE=on go get -d github.com/gogo/protobuf
GOGO_MOD=$(GO111MODULE=on go list -m github.com/gogo/protobuf)
GOGO_PATH=$GOPATH/pkg/mod/${GOGO_MOD// /@}
if [ ! -d ${GOGO_PATH} ]; then
    echo "${GOGO_PATH} does not exist, please ensure 'github.com/gogo/protobuf' is in go.mod"
    exit 1
fi

# we need `grpc-gateway` to generate HTTP API from gRPC API.
GRPC_GATEWAY=$TOOLS_BIN_DIR/protoc-gen-grpc-gateway$EXE
if [ ! -f ${GRPC_GATEWAY} ]; then
    echo "${GRPC_GATEWAY} does not exist, please run 'make tools_setup' first"
    exit 1
fi

# fetch `github.com/googleapis/googleapis`, seems no `.go` file in it,
# so neither `go.mod` nor `retool` can track it.
# hardcode the version here now.
GO111MODULE=on go get github.com/googleapis/googleapis@91ef2d9
GAPI_MOD=$(GO111MODULE=on go list -m github.com/googleapis/googleapis)
GAPI_PATH=$GOPATH/pkg/mod/${GAPI_MOD// /@}
GO111MODULE=on go mod tidy  # keep `go.mod` and `go.sum` tidy
if [ ! -d ${GAPI_PATH} ]; then
    echo "${GAPI_PATH} does not exist, try verify it manually"
    exit 1
fi

cd dm/proto || exit 1

echo "generate dm protobuf code..."

cp -r ${GAPI_PATH}/google ./

protoc -I. -I"${GOGO_PATH}" -I"${GOGO_PATH}/protobuf" --plugin=protoc-gen-gogofaster=${GOGO_FASTER} --gogofaster_out=plugins=grpc:../pb/ *.proto

protoc -I. -I"${GOGO_PATH}" -I"${GOGO_PATH}/protobuf" --plugin=protoc-gen-grpc-gateway=${GRPC_GATEWAY} --grpc-gateway_out=logtostderr=true:../pb/ dmmaster.proto

chmod -R +w ./google  # permission is `-r--r--r--`
rm -r ./google

cd ../pb || exit 1
sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
rm -f *.bak
goimports -w *.pb.go
