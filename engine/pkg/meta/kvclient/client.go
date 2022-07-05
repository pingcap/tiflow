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

package kvclient

import (
	"github.com/pingcap/tiflow/engine/pkg/meta/extension"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient/etcdkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/meta/namespace"
)

// etcdKVClient is the implement of kv interface based on etcd
// Support namespace isolation and all kv ability
// etcdImpl -> kvPrefix+Closer -> etcdKVClient
type etcdKVClient struct {
	metaclient.Client
	metaclient.KV
	tenantID string
}

// NewPrefixKVClient return a kvclient with namespace
func NewPrefixKVClient(cli extension.KVClientEx, tenantID string) metaclient.KVClient {
	pfKV := namespace.NewPrefixKV(cli, namespace.MakeNamespacePrefix(tenantID))
	return &etcdKVClient{
		Client:   cli,
		KV:       pfKV,
		tenantID: tenantID,
	}
}

// NewKVClient return a kvclient without namespace for inner use
func NewKVClient(conf *metaclient.StoreConfig) (extension.KVClientEx, error) {
	return etcdkv.NewEtcdImpl(conf)
}
