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

package framework

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

const (
	jobManagerID = "job-manager"
	jobMasterID  = "my-master"
)

// testJobMasterImpl is a mock JobMasterImpl used to test
// the correctness of BaseJobMaster.
// TODO move testJobMasterImpl to a separate file
type testJobMasterImpl struct {
	mu sync.Mutex
	mock.Mock

	base *DefaultBaseJobMaster
}

var _ JobMasterImpl = (*testJobMasterImpl)(nil)

func (m *testJobMasterImpl) InitImpl(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) Tick(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) CloseImpl(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnMasterRecovered(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerStatusUpdated(worker WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, newStatus)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerDispatched(worker WorkerHandle, result error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, result)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerOnline(worker WorkerHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerOffline(worker WorkerHandle, reason error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, reason)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, topic, message)
	return args.Error(0)
}

func (m *testJobMasterImpl) Workload() model.RescUnit {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called()
	return args.Get(0).(model.RescUnit)
}

func (m *testJobMasterImpl) OnJobManagerMessage(topic p2p.Topic, message p2p.MessageValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(topic, message)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnOpenAPIInitialized(apiGroup *gin.RouterGroup) {
	apiGroup.GET("/status", func(c *gin.Context) {
		c.String(http.StatusOK, "success")
	})
}

func (m *testJobMasterImpl) IsJobMasterImpl() {
	panic("unreachable")
}

func (m *testJobMasterImpl) Status() frameModel.WorkerStatus {
	return frameModel.WorkerStatus{
		Code: frameModel.WorkerStatusNormal,
	}
}

// simulate the job manager to insert a job record first since job master will only update the job
func prepareInsertJob(ctx context.Context, cli pkgOrm.Client, jobID string) error {
	return cli.UpsertJob(ctx, &frameModel.MasterMetaKVData{
		ID:         jobID,
		StatusCode: frameModel.MasterStatusUninit,
	})
}

func newBaseJobMasterForTests(t *testing.T, impl JobMasterImpl) *DefaultBaseJobMaster {
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	params := masterParamListForTest{
		MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
		MessageSender:         p2p.NewMockMessageSender(),
		FrameMetaClient:       cli,
		BusinessClientConn:    metaMock.NewMockClientConn(),
		ExecutorGroup:         client.NewMockExecutorGroup(),
		ServerMasterClient:    client.NewMockServerMasterClient(gomock.NewController(t)),
	}
	dp := deps.NewDeps()
	err = dp.Provide(func() masterParamListForTest {
		return params
	})
	require.NoError(t, err)

	ctx := dcontext.Background()
	ctx = ctx.WithDeps(dp)

	epoch, err := params.FrameMetaClient.GenEpoch(ctx)
	require.NoError(t, err)

	return NewBaseJobMaster(
		ctx,
		impl,
		jobManagerID,
		jobMasterID,
		FakeTask,
		epoch,
	).(*DefaultBaseJobMaster)
}

func TestBaseJobMasterBasics(t *testing.T) {
	t.Parallel()

	jobMaster := &testJobMasterImpl{}
	base := newBaseJobMasterForTests(t, jobMaster)
	jobMaster.base = base

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobMaster.mu.Lock()
	jobMaster.On("InitImpl", mock.Anything).Return(nil)
	jobMaster.mu.Unlock()

	err := jobMaster.base.Init(ctx)
	require.NoError(t, err)

	jobMaster.mu.Lock()
	jobMaster.AssertNumberOfCalls(t, "InitImpl", 1)

	// clean status
	jobMaster.ExpectedCalls = nil
	jobMaster.Calls = nil

	jobMaster.On("Tick", mock.Anything).Return(nil)
	jobMaster.mu.Unlock()

	err = jobMaster.base.Poll(ctx)
	require.NoError(t, err)

	jobMaster.mu.Lock()
	jobMaster.AssertNumberOfCalls(t, "Tick", 1)

	// clean status
	jobMaster.ExpectedCalls = nil
	jobMaster.Calls = nil

	jobMaster.On("CloseImpl", mock.Anything).Return(nil)
	jobMaster.mu.Unlock()

	status := jobMaster.Status()
	err = jobMaster.base.Exit(ctx, ExitReasonFinished, nil, string(status.ExtBytes))
	require.NoError(t, err)

	err = jobMaster.base.Close(ctx)
	require.NoError(t, err)

	jobMaster.mu.Lock()
	jobMaster.AssertNumberOfCalls(t, "CloseImpl", 1)
	jobMaster.mu.Unlock()
}

func TestOnOpenAPIInitialized(t *testing.T) {
	t.Parallel()

	jobMaster := &testJobMasterImpl{}
	base := newBaseJobMasterForTests(t, jobMaster)
	jobMaster.base = base

	engine := gin.New()
	apiGroup := engine.Group("/api/v1/jobs/test")
	base.TriggerOpenAPIInitialize(apiGroup)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/test/status", nil)
	engine.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "success", w.Body.String())
}

func TestJobMasterExit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		exitReason       ExitReason
		err              error
		extMsg           string
		expectedStatus   frameModel.MasterStatusCode
		expectedErrorMsg string
		expectedExtMsg   string
	}{
		{
			exitReason:       ExitReasonFinished,
			err:              nil,
			extMsg:           "test finished",
			expectedStatus:   frameModel.MasterStatusFinished,
			expectedErrorMsg: "",
			expectedExtMsg:   "test finished",
		},
		{
			exitReason:       ExitReasonFinished,
			err:              errors.New("test finished with error"),
			extMsg:           "test finished",
			expectedStatus:   frameModel.MasterStatusFinished,
			expectedErrorMsg: "test finished with error",
			expectedExtMsg:   "test finished",
		},
		{
			exitReason:       ExitReasonCanceled,
			err:              nil,
			extMsg:           "test canceled",
			expectedStatus:   frameModel.MasterStatusStopped,
			expectedErrorMsg: "",
			expectedExtMsg:   "test canceled",
		},
		{
			exitReason:       ExitReasonCanceled,
			err:              errors.New("test canceled with error"),
			extMsg:           "test canceled",
			expectedStatus:   frameModel.MasterStatusStopped,
			expectedErrorMsg: "test canceled with error",
			expectedExtMsg:   "test canceled",
		},
		{
			exitReason:       ExitReasonFailed,
			err:              nil,
			extMsg:           "test failed",
			expectedStatus:   frameModel.MasterStatusFailed,
			expectedErrorMsg: "",
			expectedExtMsg:   "test failed",
		},
		{
			exitReason:       ExitReasonFailed,
			err:              errors.New("test failed with error"),
			extMsg:           "test failed",
			expectedStatus:   frameModel.MasterStatusFailed,
			expectedErrorMsg: "test failed with error",
			expectedExtMsg:   "test failed",
		},
	}

	for _, cs := range cases {
		jobMaster := &testJobMasterImpl{}
		base := newBaseJobMasterForTests(t, jobMaster)
		jobMaster.base = base
		require.Equal(t, jobMasterID, jobMaster.base.ID())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := prepareInsertJob(ctx, base.master.frameMetaClient, jobMaster.base.ID())
		require.NoError(t, err)

		jobMaster.mu.Lock()
		jobMaster.On("InitImpl", mock.Anything).Return(nil)
		jobMaster.mu.Unlock()

		err = jobMaster.base.Init(ctx)
		require.NoError(t, err)

		metas, err := jobMaster.base.master.frameMetaClient.QueryJobs(ctx)
		require.NoError(t, err)
		require.Len(t, metas, 1)

		jobMaster.mu.Lock()
		jobMaster.AssertNumberOfCalls(t, "InitImpl", 1)

		// clean status
		jobMaster.ExpectedCalls = nil
		jobMaster.Calls = nil

		jobMaster.On("Tick", mock.Anything).Return(nil)
		jobMaster.mu.Unlock()

		err = jobMaster.base.Poll(ctx)
		require.NoError(t, err)

		jobMaster.mu.Lock()
		jobMaster.AssertNumberOfCalls(t, "Tick", 1)

		// clean status
		jobMaster.ExpectedCalls = nil
		jobMaster.Calls = nil

		jobMaster.On("CloseImpl", mock.Anything).Return(nil)
		jobMaster.mu.Unlock()

		// test exit status
		err = jobMaster.base.Exit(ctx, cs.exitReason, cs.err, cs.extMsg)
		require.NoError(t, err)
		meta, err := jobMaster.base.master.frameMetaClient.GetJobByID(ctx, jobMaster.base.ID())
		require.NoError(t, err)
		require.Equal(t, cs.expectedStatus, meta.StatusCode)
		require.Equal(t, cs.expectedExtMsg, meta.ExtMsg)

		err = jobMaster.base.Close(ctx)
		require.NoError(t, err)

		jobMaster.mu.Lock()
		jobMaster.AssertNumberOfCalls(t, "CloseImpl", 1)
		jobMaster.mu.Unlock()
	}
}
