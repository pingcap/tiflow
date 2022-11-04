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

package mockkv

import (
	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// ClientBuilderImpl is the mock kvclient builder
type ClientBuilderImpl struct{}

// ClientType implements ClientType of clientBuilder
func (b *ClientBuilderImpl) ClientType() metaModel.ClientType {
	return metaModel.MockKVClientType
}

// NewKVClientWithNamespace implements NewKVClientWithNamespace of clientBuilder
func (b *ClientBuilderImpl) NewKVClientWithNamespace(cc metaModel.ClientConn,
	projectID metaModel.ProjectID, jobID metaModel.JobID,
) (metaModel.KVClient, error) {
	if cc.StoreType() != metaModel.StoreTypeMockKV {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack("invalid ClientConn for etcd kvclient builder,"+
			" conn type:%s", cc.StoreType())
	}

	return metaMock.NewMetaMock(), nil
}
