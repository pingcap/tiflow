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
	"fmt"
	"sync"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
)

// NewClientConnImpl return a new clientConnImpl
func NewClientConnImpl() *clientConnImpl {
	return &clientConnImpl{
		isInitialized: atomic.NewBool(false),
	}
}

type clientConnImpl struct {
	mu            sync.Mutex
	isInitialized *atomic.Bool
	cli           *clientv3.Client
}

// Initialize implements Initialize of ClientConn
func (cc *clientConnImpl) Initialize(storeConf *metaModel.StoreConfig) error {
	if storeConf == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("storeConf is nil")
	}

	if storeConf.StoreType != metaModel.StoreTypeEtcd {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs(fmt.Sprintf(
			"etcd conn but get unmatch type:%s", storeConf.StoreType))
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.isInitialized.Load() {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs("already initialized")
	}

	etcdCli, err := NewEtcdClient(storeConf)
	if err != nil {
		return err
	}

	cc.isInitialized.Store(true)
	cc.cli = etcdCli
	return nil
}

// GetConn implements GetConn of ClientConn
func (cc *clientConnImpl) GetConn() (interface{}, error) {
	if cc.isInitialized.Load() == false {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs("connection is uninitialized")
	}

	return cc.cli, nil
}

// Close implements Close of ClientConn
func (cc *clientConnImpl) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.isInitialized.Store(false)
	if cc.cli != nil {
		cc.cli.Close()
		cc.cli = nil
	}

	return nil
}

// ClientType implements ClientType of ClientConn
func (cc *clientConnImpl) ClientType() metaModel.ClientType {
	return metaModel.EtcdKVClientType
}

// NewEtcdClient new a clientv3.Client.For easy test usage
func NewEtcdClient(storeConf *metaModel.StoreConfig) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: storeConf.Endpoints,
		// [TODO] TLS, AUTH
		// [TODO] LOG
	})
	if err != nil {
		return nil, errors.ErrMetaNewClientFail.Wrap(err)
	}

	return cli, err
}
