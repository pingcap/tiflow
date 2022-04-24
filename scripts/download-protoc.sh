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
GOGO_VERSION=1.3.2
PROTOC_VERSION=3.20.1
OS="$(uname)"

case $OS in
'Linux')
	PROTOC_URL=https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip
	;;
'Darwin')
	if [[ $(uname -m) == 'x86_64' ]]; then
		PROTOC_URL=https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-osx-x86_64.zip
	else
		PROTOC_URL=https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-osx-aarch_64.zip
	fi
	;;
*)
	echo "only supports Linux and macOS"
	exit 1
	;;
esac

echo "download gogo.proto..."
[ ! -d $TOOLS_INCLUDE_DIR/gogoproto ] && mkdir -p $TOOLS_INCLUDE_DIR/gogoproto
[ ! -f $TOOLS_INCLUDE_DIR/gogoproto/gogo.proto ] &&
	curl -sL https://raw.githubusercontent.com/gogo/protobuf/v$GOGO_VERSION/gogoproto/gogo.proto \
		-o $TOOLS_INCLUDE_DIR/gogoproto/gogo.proto

echo "download protoc..."
[ ! -d $TOOLS_BIN_DIR ] && mkdir -p $TOOLS_BIN_DIR
[ ! -f $TOOLS_BIN_DIR/protoc ] &&
	mkdir -p /tmp/cdc/protoc &&
	curl -sL $PROTOC_URL -o /tmp/cdc/protoc/protoc-$PROTOC_VERSION-linux-x86_64.zip &&
	unzip -q -o -d /tmp/cdc/protoc /tmp/cdc/protoc/protoc-$PROTOC_VERSION-linux-x86_64.zip &&
	mv /tmp/cdc/protoc/include/google $TOOLS_INCLUDE_DIR &&
	mv /tmp/cdc/protoc/bin/protoc $TOOLS_BIN_DIR/protoc
