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

package metadata

import (
	"context"

	"github.com/coreos/go-semver/semver"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"go.uber.org/zap"
)

// MetaData is the metadata of dm.
type MetaData struct {
	clusterInfoStore *ClusterInfoStore
	jobStore         *JobStore
	ddlStore         *DDLStore
	unitStateStore   *UnitStateStore
}

// NewMetaData creates a new MetaData instance
func NewMetaData(kvClient metaModel.KVClient, pLogger *zap.Logger) *MetaData {
	return &MetaData{
		clusterInfoStore: NewClusterInfoStore(kvClient),
		jobStore:         NewJobStore(kvClient, pLogger),
		ddlStore:         NewDDLStore(kvClient),
		unitStateStore:   NewUnitStateStore(kvClient),
	}
}

// ClusterInfoStore returns internal infoStore
func (m *MetaData) ClusterInfoStore() *ClusterInfoStore {
	return m.clusterInfoStore
}

// JobStore returns internal jobStore
func (m *MetaData) JobStore() *JobStore {
	return m.jobStore
}

// DDLStore returns internal ddlStore
func (m *MetaData) DDLStore() *DDLStore {
	return m.ddlStore
}

// UnitStateStore returns internal unitStateStore
func (m *MetaData) UnitStateStore() *UnitStateStore {
	return m.unitStateStore
}

// Upgrade upgrades metadata.
func (m *MetaData) Upgrade(ctx context.Context, fromVer semver.Version) error {
	// call infoStore.Upgrade/ddlStore.Upgrade if needed.
	return m.jobStore.Upgrade(ctx, fromVer)
}
