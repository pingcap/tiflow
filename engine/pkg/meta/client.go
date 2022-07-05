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

package meta

import (
	"github.com/pingcap/tiflow/engine/pkg/meta/internal"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// NewKVClientWithNamespace return a KVClient with namspace isolation
func NewKVClientWithNamespace(tp metaModel.ClientType, storeConf *metaModel.StoreConfig,
	projectID metaModel.ProjectID, jobID metaModel.JobID) (metaModel.KVClient, error) {
	builder, err := internal.GetClientBuilder(tp)
	if err != nil {
		return nil, err
	}

	return builder.NewKVClientWithNamespace(storeConf, projectID, jobID)
}

// NewKVClient return a KVClient without namespace isolation
func NewKVClient(tp metaModel.ClientType, storeConf *metaModel.StoreConfig) (metaModel.KVClient, error) {
	builder, err := internal.GetClientBuilder(tp)
	if err != nil {
		return nil, err
	}

	return builder.NewKVClient(storeConf)
}
