// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metaclient

import (
	"github.com/pingcap/tiflow/engine/pkg/meta/internal"
	"github.com/pingcap/tiflow/engine/pkg/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

type ClientType int

type (
	ProjectID = tenant.ProjectID
	JobID     = model.JobID
)

const (
	UnKnownKVClientType = ClientType(iota)
	EtcdKVClientType
	SQLKVClientType
)

func NewKVClientWithNamespace(tp ClientType, storeConf *StoreConfigParams,
	projectID ProjectID, jobID JobID) (KVClient, error) {
	builder, err := internal.GetClientBuilder(tp)
	if err != nil {
		return nil, err
	}

	return builder.NewKVClientWithNamespace(storeConf, projectID, jobID)
}

func NewKVClient(tp ClientType, storeConf *StoreConfigParams) (KVClient, error) {
	builder, err := internal.GetClientBuilder(tp)
	if err != nil {
		return nil, err
	}

	return builder.NewKVClient(storeConf)
}
