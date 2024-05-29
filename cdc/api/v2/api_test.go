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

package v2

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	pd "github.com/tikv/pd/client"
)

type testCase struct {
	url    string
	method string
}

func newRouter(apiV2 OpenAPIV2) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIV2Routes(router, apiV2)
	return router
}

// mockPDClient mocks pd.Client to facilitate unit testing.
type mockPDClient struct {
	pd.Client
	logicTime int64
	timestamp int64
}

// UpdateServiceGCSafePoint mocks the corresponding method of a real PDClient
func (c *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context,
	serviceID string, ttl int64, safePoint uint64,
) (uint64, error) {
	return safePoint, nil
}

// GetTS of mockPDClient returns a mock tso
func (c *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return c.timestamp, c.logicTime, nil
}

// GetClusterID of mockPDClient returns a mock ClusterID
func (c *mockPDClient) GetClusterID(ctx context.Context) uint64 {
	return 123
}

// Close mocks the Close() method of a PDClient
func (c *mockPDClient) Close() {}

type mockStatusProvider struct {
	owner.StatusProvider
	changefeedStatus       *model.ChangeFeedStatusForAPI
	changefeedInfo         *model.ChangeFeedInfo
	processors             []*model.ProcInfoSnap
	taskStatus             map[model.CaptureID]*model.TaskStatus
	changefeedInfos        map[model.ChangeFeedID]*model.ChangeFeedInfo
	changefeedStatuses     map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI
	changeFeedSyncedStatus *model.ChangeFeedSyncedStatusForAPI
	err                    error
}

// GetChangeFeedStatus returns a changefeeds' runtime status.
func (m *mockStatusProvider) GetChangeFeedStatus(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedStatusForAPI, error) {
	return m.changefeedStatus, m.err
}

// GetChangeFeedStatus returns a mock changefeeds' info.
func (m *mockStatusProvider) GetChangeFeedInfo(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedInfo, error) {
	return m.changefeedInfo, m.err
}

// GetProcessors returns a list of mock processor infos.
func (m *mockStatusProvider) GetProcessors(ctx context.Context) (
	[]*model.ProcInfoSnap,
	error,
) {
	return m.processors, m.err
}

// GetAllTaskStatuses returns a map of mock task statuses.
func (m *mockStatusProvider) GetAllTaskStatuses(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
) (
	map[model.CaptureID]*model.TaskStatus,
	error,
) {
	return m.taskStatus, m.err
}

// GetAllChangeFeedInfo returns a list of mock changefeed info.
func (m *mockStatusProvider) GetAllChangeFeedInfo(_ context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedInfo,
	error,
) {
	return m.changefeedInfos, m.err
}

// GetAllChangeFeedStatuses returns a list of mock changefeed status.
func (m *mockStatusProvider) GetAllChangeFeedStatuses(_ context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI,
	error,
) {
	return m.changefeedStatuses, m.err
}

// GetChangeFeedSyncedStatus returns a mock changefeed status.
func (m *mockStatusProvider) GetChangeFeedSyncedStatus(_ context.Context, changefeedID model.ChangeFeedID) (
	*model.ChangeFeedSyncedStatusForAPI,
	error,
) {
	return m.changeFeedSyncedStatus, m.err
}
