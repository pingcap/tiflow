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
	"context"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/meta/extension"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient/etcdkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/meta/namespace"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
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
func NewKVClient(conf *metaclient.StoreConfigParams) (extension.KVClientEx, error) {
	return etcdkv.NewEtcdImpl(conf)
}

// CheckAccessForMetaStore check the connectivity of the specify metastore
func CheckAccessForMetaStore(conf *metaclient.StoreConfigParams) error {
	cliEx, err := NewKVClient(conf)
	if err != nil {
		return err
	}
	defer cliEx.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli := NewPrefixKVClient(cliEx, tenant.TestTenantID)
	_, err = cli.Put(ctx, "test_key", "test_value")
	if err != nil {
		return err
	}

	return nil
}
