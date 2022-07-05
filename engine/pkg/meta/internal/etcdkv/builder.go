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
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/etcdkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv/namespace"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

func init() {
	internal.MustRegister(&etcdKVClientBuilder{})
}

type etcdKVClientBuilder struct{}

func (b *etcdKVClientBuilder) ClientType() {
	return metaModel.EtcdKVClientType
}

func (b *etcdKVClientBuilder) NewKVClientWithNamespace(storeConf *metaModel.StoreConfig,
	projectID metaModel.ProjectID, jobID metaModel.JobID) (metaModel.KVClient, error) {
	cli := etcdkv.NewEtcdImpl(conf)
	pfKV := namespace.NewPrefixKV(cli, namespace.MakeNamespacePrefix(projectID, jobID))
	return &etcdKVClient{
		Client: cli,
		KV:     pfKV,
	}
}

func (b *etcdKVClientBuilder) NewKVClient(conf *metaModel.StoreConfig) (metaModel.KVClient, error) {
	return etcdkv.NewEtcdImpl(conf)
}

// etcdKVClient is the implement of kv interface based on etcd
// Support namespace isolation and all kv ability
// etcdImpl -> kvPrefix+Closer -> etcdKVClient
type etcdKVClient struct {
	metaModel.Client
	metaModel.KV
}
