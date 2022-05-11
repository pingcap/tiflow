package metadata

import (
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

// MetaData is the metadata of dm.
type MetaData struct {
	jobStore *JobStore
	ddlStore *DDLStore
}

// NewMetaData creates a new MetaData instance
func NewMetaData(id libModel.WorkerID, kvClient metaclient.KVClient) *MetaData {
	return &MetaData{
		jobStore: NewJobStore(id, kvClient),
		ddlStore: NewDDLStore(id, kvClient),
	}
}

// JobStore returns internal jobStore
func (m *MetaData) JobStore() *JobStore {
	return m.jobStore
}

// DDLStore returns internal ddlStore
func (m *MetaData) DDLStore() *DDLStore {
	return m.ddlStore
}
