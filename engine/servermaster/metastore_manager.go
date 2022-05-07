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

package servermaster

import (
	"sync"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"go.uber.org/zap"
)

type MetaStoreManager interface {
	// Register register specify backend store to manager with an unique id
	// id can be some readable identifier, like `meta-test1`.
	// Duplicate id will return an error
	Register(id string, store *metaclient.StoreConfigParams) error
	// UnRegister delete an existing backend store
	UnRegister(id string)
	// GetMetaStore get an existing backend store info
	GetMetaStore(id string) *metaclient.StoreConfigParams
}

type metaStoreManagerImpl struct {
	// From id to metaclient.StoreConfigParams
	id2Store sync.Map
}

func NewMetaStoreManager() MetaStoreManager {
	return &metaStoreManagerImpl{}
}

func (m *metaStoreManagerImpl) Register(id string, store *metaclient.StoreConfigParams) error {
	if _, exists := m.id2Store.LoadOrStore(id, store); exists {
		log.L().Error("register metastore fail", zap.Any("config", store), zap.String("err", "Duplicate storeID"))
		return errors.ErrMetaStoreIDDuplicate.GenWithStackByArgs()
	}
	return nil
}

func (m *metaStoreManagerImpl) UnRegister(id string) {
	m.id2Store.Delete(id)
	log.L().Info("unregister metastore", zap.String("storeID", id))
}

func (m *metaStoreManagerImpl) GetMetaStore(id string) *metaclient.StoreConfigParams {
	if store, exists := m.id2Store.Load(id); exists {
		return store.(*metaclient.StoreConfigParams)
	}

	return nil
}

// NewFrameMetaConfig return the default framework metastore config
func NewFrameMetaConfig() *metaclient.StoreConfigParams {
	return &metaclient.StoreConfigParams{
		StoreID: metaclient.FrameMetaID,
		Endpoints: []string{
			pkgOrm.DefaultFrameMetaEndpoints,
		},
		Auth: metaclient.AuthConfParams{
			User:   pkgOrm.DefaultFrameMetaUser,
			Passwd: pkgOrm.DefaultFrameMetaPassword,
		},
	}
}

// NewDefaultUserMetaConfig return the default user metastore config
func NewDefaultUserMetaConfig() *metaclient.StoreConfigParams {
	return &metaclient.StoreConfigParams{
		StoreID: metaclient.DefaultUserMetaID,
		Endpoints: []string{
			metaclient.DefaultUserMetaEndpoints,
		},
	}
}
