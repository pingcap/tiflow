package servermaster

import (
	"sync"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// MetaStoreManager defines an interface to manage metastore
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

// NewMetaStoreManager creates a new metaStoreManagerImpl instance
func NewMetaStoreManager() MetaStoreManager {
	return &metaStoreManagerImpl{}
}

// Register implements MetaStoreManager.Register
func (m *metaStoreManagerImpl) Register(id string, store *metaclient.StoreConfigParams) error {
	if _, exists := m.id2Store.LoadOrStore(id, store); exists {
		log.L().Error("register metastore fail", zap.Any("config", store), zap.String("err", "Duplicate storeID"))
		return errors.ErrMetaStoreIDDuplicate.GenWithStackByArgs()
	}
	return nil
}

// Unregister implements MetaStoreManager.Unregister
func (m *metaStoreManagerImpl) UnRegister(id string) {
	m.id2Store.Delete(id)
	log.L().Info("unregister metastore", zap.String("storeID", id))
}

// GetMetaStore implements MetaStoreManager.GetMetaStore
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
