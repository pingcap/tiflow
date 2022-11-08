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

package etcdkv

import (
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv/namespace"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ClientBuilderImpl is the etcd kvclient builder
type ClientBuilderImpl struct{}

// ClientType implements ClientType of clientBuilder
func (b *ClientBuilderImpl) ClientType() metaModel.ClientType {
	return metaModel.EtcdKVClientType
}

// NewKVClientWithNamespace implements NewKVClientWithNamespace of clientBuilder
func (b *ClientBuilderImpl) NewKVClientWithNamespace(cc metaModel.ClientConn,
	projectID metaModel.ProjectID, jobID metaModel.JobID,
) (metaModel.KVClient, error) {
	cli, err := cc.GetConn()
	if err != nil {
		return nil, err
	}

	etcdCli, ok := cli.(*clientv3.Client)
	if !ok {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack("invalid ClientConn type for etcd kvclient builder,"+
			" conn type:%s", cc.StoreType())
	}
	impl, err := NewEtcdKVClientImpl(etcdCli)
	if err != nil {
		return nil, err
	}
	pfKV := namespace.NewPrefixKV(impl, namespace.MakeNamespacePrefix(projectID, jobID))
	return &etcdKVClient{
		Client: impl,
		KV:     pfKV,
	}, nil
}

// etcdKVClient is the implement of kv interface based on etcd
// Support namespace isolation and all kv ability
// etcdKVClientImpl -> kvPrefix+Closer -> etcdKVClient
type etcdKVClient struct {
	metaModel.Client
	metaModel.KV
}
