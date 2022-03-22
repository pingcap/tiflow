package metadata

import (
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

// DDL represents the state of ddls.
// TODO: implement DDL
type DDL struct {
	State
}

// DDLStore manages the state of ddls.
// Write by DDLCoordinator.
type DDLStore struct {
	*DefaultStore

	id lib.MasterID
}

func NewDDLStore(id lib.MasterID, kvClient metaclient.KVClient) *DDLStore {
	ddlStore := &DDLStore{
		DefaultStore: NewDefaultStore(kvClient),
		id:           id,
	}
	ddlStore.DefaultStore.Store = ddlStore
	return ddlStore
}

func (ddlStore *DDLStore) CreateState() State {
	return &DDL{}
}

// TODO: add ddl key
func (ddlStore *DDLStore) Key() string {
	return ""
}
