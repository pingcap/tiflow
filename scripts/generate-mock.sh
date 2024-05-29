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

# CDC mock
"$MOCKGEN" -source cdc/owner/owner.go -destination cdc/owner/mock/owner_mock.go
"$MOCKGEN" -source cdc/owner/status_provider.go -destination cdc/owner/mock/status_provider_mock.go
"$MOCKGEN" -source cdc/api/v2/api_helpers.go -destination cdc/api/v2/api_helpers_mock.go -package v2
"$MOCKGEN" -source pkg/etcd/etcd.go -destination pkg/etcd/mock/etcd_client_mock.go
"$MOCKGEN" -source cdc/processor/manager.go -destination cdc/processor/mock/manager_mock.go
"$MOCKGEN" -source cdc/capture/capture.go -destination cdc/capture/mock/capture_mock.go
"$MOCKGEN" -source pkg/cmd/factory/factory.go -destination pkg/cmd/factory/mock/factory_mock.go -package mock_factory
"$MOCKGEN" -source cdc/processor/sourcemanager/sorter/engine.go -destination cdc/processor/sourcemanager/sorter/mock/engine_mock.go
"$MOCKGEN" -source pkg/api/v2/changefeed.go -destination pkg/api/v2/mock/changefeed_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/tso.go -destination pkg/api/v2/mock/tso_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/unsafe.go -destination pkg/api/v2/mock/unsafe_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/status.go -destination pkg/api/v2/mock/status_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/capture.go -destination pkg/api/v2/mock/capture_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/processor.go -destination pkg/api/v2/mock/processor_mock.go -package mock
"$MOCKGEN" -source pkg/sink/kafka/v2/client.go -destination pkg/sink/kafka/v2/mock/client_mock.go
"$MOCKGEN" -source pkg/sink/kafka/v2/gssapi.go -destination pkg/sink/kafka/v2/mock/gssapi_mock.go
"$MOCKGEN" -source pkg/sink/kafka/v2/writer.go -destination pkg/sink/kafka/v2/mock/writer_mock.go
"$MOCKGEN" -source pkg/sink/codec/simple/marshaller.go -destination pkg/sink/codec/simple/mock/marshaller.go

# DM mock
"$MOCKGEN" -package pbmock -destination dm/pbmock/dmmaster.go github.com/pingcap/tiflow/dm/pb MasterClient,MasterServer
"$MOCKGEN" -package pbmock -destination dm/pbmock/dmworker.go github.com/pingcap/tiflow/dm/pb WorkerClient,WorkerServer

# Engine mock
"$MOCKGEN" -package mock -destination engine/pkg/meta/mock/client_mock.go github.com/pingcap/tiflow/engine/pkg/meta/model KVClient
"$MOCKGEN" -package mock -destination engine/executor/server/mock/metastore_mock.go github.com/pingcap/tiflow/engine/executor/server MetastoreCreator
"$MOCKGEN" -package mock -destination engine/enginepb/mock/executor_mock.go github.com/pingcap/tiflow/engine/enginepb ExecutorServiceClient
"$MOCKGEN" -package mock -destination engine/enginepb/mock/broker_mock.go github.com/pingcap/tiflow/engine/enginepb BrokerServiceClient
"$MOCKGEN" -package mock -destination engine/enginepb/mock/resource_mock.go github.com/pingcap/tiflow/engine/enginepb ResourceManagerClient
"$MOCKGEN" -package mock -destination engine/pkg/httputil/mock/jobhttpclient_mock.go github.com/pingcap/tiflow/engine/pkg/httputil JobHTTPClient
"$MOCKGEN" -package mock -destination engine/servermaster/jobop/mock/joboperator_mock.go github.com/pingcap/tiflow/engine/servermaster/jobop JobOperator
"$MOCKGEN" -package mock -destination engine/pkg/orm/mock/client_mock.go github.com/pingcap/tiflow/engine/pkg/orm Client
"$MOCKGEN" -package mock -destination engine/servermaster/jobop/mock/backoffmanager_mock.go github.com/pingcap/tiflow/engine/servermaster/jobop BackoffManager
"$MOCKGEN" -package mock -source engine/pkg/rpcutil/checker.go -destination engine/pkg/rpcutil/mock/checker_mock.go
"$MOCKGEN" -package client -self_package github.com/pingcap/tiflow/engine/pkg/client \
	-destination engine/pkg/client/client_mock.go github.com/pingcap/tiflow/engine/pkg/client ExecutorClient,ServerMasterClient

# PKG mock
"$MOCKGEN" -package mock -destination pkg/election/mock/storage_mock.go github.com/pingcap/tiflow/pkg/election Storage
"$MOCKGEN" -package mock -destination pkg/election/mock/elector_mock.go github.com/pingcap/tiflow/pkg/election Elector
