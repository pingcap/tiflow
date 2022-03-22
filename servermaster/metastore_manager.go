package servermaster

import (
	"sync"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
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
		return errors.ErrMetaStoreIDDuplicate.GenWithStackByArgs()
	}
	return nil
}

func (m *metaStoreManagerImpl) UnRegister(id string) {
	m.id2Store.Delete(id)
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
			metaclient.DefaultFrameMetaEndpoints,
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
