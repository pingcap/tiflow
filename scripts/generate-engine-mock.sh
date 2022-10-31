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

cd "$(dirname "${BASH_SOURCE[0]}")"/..

MOCKGEN="tools/bin/mockgen"

if [ ! -f "$MOCKGEN" ]; then
	echo "${MOCKGEN} does not exist, please run 'make tools/bin/mockgen' first"
	exit 1
fi

echo "generate dataflow engine mock code..."

"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/pkg/meta/model KVClient \
	>engine/pkg/meta/mock/client_mock.go
"$MOCKGEN" -package mocks github.com/pingcap/tiflow/engine/executor/server MetastoreCreator \
	>engine/executor/server/mocks/metastore_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/enginepb ExecutorServiceClient \
	>engine/enginepb/mock/executor_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/enginepb BrokerServiceClient \
	>engine/enginepb/mock/broker_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/enginepb ResourceManagerClient \
	>engine/enginepb/mock/resource_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/pkg/httputil JobHTTPClient \
	>engine/pkg/httputil/mock/jobhttpclient_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/servermaster/jobop JobOperator \
	>engine/servermaster/jobop/mock/joboperator_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/pkg/election Storage \
	>engine/pkg/election/mock/storage_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/pkg/orm Client \
	>engine/pkg/orm/mock/client_mock.go
"$MOCKGEN" -package mock github.com/pingcap/tiflow/engine/servermaster/jobop BackoffManager \
	>engine/servermaster/jobop/mock/backoffmanager_mock.go

rm engine/pkg/client/client_mock.go || true
"$MOCKGEN" -package client -self_package github.com/pingcap/tiflow/engine/pkg/client \
	github.com/pingcap/tiflow/engine/pkg/client ExecutorClient,ServerMasterClient \
	>engine/pkg/client/client_mock.go_temp
mv engine/pkg/client/client_mock.go_temp engine/pkg/client/client_mock.go

echo "generate dataflow engine mock code successfully"
