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

func NewMetaData(id libModel.WorkerID, kvClient metaclient.KVClient) *MetaData {
	return &MetaData{
		jobStore: NewJobStore(id, kvClient),
		ddlStore: NewDDLStore(id, kvClient),
	}
}

func (m *MetaData) JobStore() *JobStore {
	return m.jobStore
}

func (m *MetaData) DDLStore() *DDLStore {
	return m.ddlStore
}
