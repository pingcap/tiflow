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

package dm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUnitHolder(t *testing.T) {
	unitHolder := newUnitHolderImpl(lib.WorkerDMDump, &config.SubTaskConfig{Name: "job-id", SourceID: "task-id", Flavor: mysql.MySQLFlavor})
	require.IsType(t, &dumpling.Dumpling{}, unitHolder.unit)
	unitHolder = newUnitHolderImpl(lib.WorkerDMLoad, &config.SubTaskConfig{Name: "job-id", SourceID: "task-id", Flavor: mysql.MySQLFlavor})
	require.IsType(t, &loader.LightningLoader{}, unitHolder.unit)
	unitHolder = newUnitHolderImpl(lib.WorkerDMSync, &config.SubTaskConfig{Name: "job-id", SourceID: "task-id", Flavor: mysql.MySQLFlavor})
	require.IsType(t, &syncer.Syncer{}, unitHolder.unit)

	u := &mockUnit{}
	unitHolder.unit = u
	u.On("Init").Return(errors.New("error")).Once()
	require.Error(t, unitHolder.Init(context.Background()))
	u.On("Init").Return(nil).Once()
	require.NoError(t, unitHolder.Init(context.Background()))
	require.Equal(t, metadata.StageRunning, unitHolder.Stage())

	require.NoError(t, unitHolder.Tick(context.Background()))
	unitHolder.resultCh <- pb.ProcessResult{Errors: []*pb.ProcessError{{
		ErrCode: 0,
	}}}
	require.NoError(t, unitHolder.Tick(context.Background()))
	require.Equal(t, metadata.StagePaused, unitHolder.Stage())
	time.Sleep(time.Second)
	require.NoError(t, unitHolder.Tick(context.Background()))
	require.Equal(t, metadata.StageRunning, unitHolder.Stage())

	u.On("Status").Return(&pb.DumpStatus{})
	status := unitHolder.Status(context.Background())
	require.Equal(t, &pb.DumpStatus{}, status)

	unitHolder.resultCh <- pb.ProcessResult{Errors: []*pb.ProcessError{{
		ErrCode: 0,
		Message: "context canceled",
	}}}
	require.NoError(t, unitHolder.Tick(context.Background()))
	require.Equal(t, metadata.StageFinished, unitHolder.Stage())
	require.NoError(t, unitHolder.Close(context.Background()))
}

type mockUnit struct {
	sync.Mutex
	mock.Mock
}

// mockUnitHolder implement Holder
type mockUnitHolder struct {
	sync.Mutex
	mock.Mock
}

func (u *mockUnit) Init(ctx context.Context) error {
	u.Lock()
	defer u.Unlock()
	return u.Called().Error(0)
}

func (u *mockUnit) Process(ctx context.Context, pr chan pb.ProcessResult) {}

func (u *mockUnit) Close() {}

func (u *mockUnit) Kill() {}

func (u *mockUnit) Pause() {}

func (u *mockUnit) Resume(ctx context.Context, pr chan pb.ProcessResult) {}

func (u *mockUnit) Update(ctx context.Context, cfg *config.SubTaskConfig) error {
	return nil
}

func (u *mockUnit) Status(sourceStatus *binlog.SourceStatus) interface{} {
	u.Lock()
	defer u.Unlock()
	return u.Called().Get(0)
}

func (u *mockUnit) Type() pb.UnitType {
	u.Lock()
	defer u.Unlock()
	return u.Called().Get(0).(pb.UnitType)
}

func (u *mockUnit) IsFreshTask(ctx context.Context) (bool, error) {
	return false, nil
}

// Init implement Holder.Init
func (m *mockUnitHolder) Init(ctx context.Context) error {
	return nil
}

// Tick implement Holder.Tick
func (m *mockUnitHolder) Tick(ctx context.Context) error {
	return nil
}

// Stage implement Holder.Stage
func (m *mockUnitHolder) Stage() metadata.TaskStage {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0).(metadata.TaskStage)
}

// Status implement Holder.Status
func (m *mockUnitHolder) Status(ctx context.Context) interface{} {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0)
}

// Close implement Holder.Close
func (m *mockUnitHolder) Close(ctx context.Context) error {
	return nil
}
