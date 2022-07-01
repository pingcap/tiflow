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

package extension

import (
	"context"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

// KVExt extends the KV interface with Do method to implement the intermediate
// layer easier
type KVExt interface {
	metaclient.KV

	// Do applies a single Op on KV without a transaction.
	// Do is useful when adding intermidate layer to KV implement
	Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, metaclient.Error)
}

type ClientBuilder interface {
	ClientType() metaclient.ClientType
	NewKVClientWithNamespace(storeConf *metaclient.StoreConfigParams,
		projectID tenant.projectID, jobID model.JobID) (metaclient.KVClient, error)
	NewKVClient(storeConf *metaclient.StoreConfigParams) (metaclient.KVClient, error)
}
