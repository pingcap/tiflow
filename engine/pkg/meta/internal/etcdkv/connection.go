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
	"sync"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewClientConnImpl return a new clientConnImpl
func NewClientConnImpl(storeConf *metaModel.StoreConfig) (*clientConnImpl, error) {
	if storeConf == nil {
		return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("store config is nil")
	}

	if storeConf.StoreType != metaModel.StoreTypeEtcd {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack(
			"etcd conn but get unmatch type:%s", storeConf.StoreType)
	}

	etcdCli, err := NewEtcdClient(storeConf)
	if err != nil {
		return nil, err
	}

	return &clientConnImpl{
		cli: etcdCli,
	}, nil
}

type clientConnImpl struct {
	rwLock sync.RWMutex
	cli    *clientv3.Client
}

// GetConn implements GetConn of ClientConn
func (cc *clientConnImpl) GetConn() (interface{}, error) {
	cc.rwLock.RLock()
	defer cc.rwLock.RUnlock()

	if cc.cli == nil {
		return nil, errors.ErrMetaOpFail.GenWithStackByArgs("connection is uninitialized")
	}

	return cc.cli, nil
}

// Close implements Close of ClientConn
func (cc *clientConnImpl) Close() error {
	cc.rwLock.Lock()
	defer cc.rwLock.Unlock()

	if cc.cli != nil {
		cc.cli.Close()
		cc.cli = nil
	}

	return nil
}

// StoreType implements StoreType of ClientConn
func (cc *clientConnImpl) StoreType() metaModel.StoreType {
	return metaModel.StoreTypeEtcd
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
