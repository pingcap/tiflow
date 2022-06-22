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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUnitHolder(t *testing.T) {
	unitHolder := &unitHolderImpl{}
	u := &mockUnit{}
	unitHolder.unit = u
	u.On("Init").Return(errors.New("error")).Once()
	require.Error(t, unitHolder.Init(context.Background()))
	u.On("Init").Return(nil).Once()
	require.NoError(t, unitHolder.Init(context.Background()))
	stage, result := unitHolder.Stage()
	require.Nil(t, result)
	require.Equal(t, metadata.StageRunning, stage)

	// mock error
	time.Sleep(time.Second)
	u.setResult(pb.ProcessResult{Errors: []*pb.ProcessError{{
		ErrCode: 0,
	}}})
	unitHolder.processWg.Wait()
	stage, result = unitHolder.Stage()
	require.Equal(t, 0, int(result.Errors[0].ErrCode))
	require.Equal(t, metadata.StageError, stage)

	// mock auto resume
	require.NoError(t, unitHolder.Resume(context.Background()))
	stage, result = unitHolder.Stage()
	require.Nil(t, result)
	require.Equal(t, metadata.StageRunning, stage)

	// mock paused
	go func() {
		time.Sleep(time.Second)
		u.setResult(pb.ProcessResult{Errors: []*pb.ProcessError{{
			Message: "context canceled",
		}}})
	}()
	// mock pausing
	go func() {
		require.Eventually(t, func() bool {
			stage, _ := unitHolder.Stage()
			return stage == metadata.StagePausing
		}, 5*time.Second, 100*time.Millisecond)
	}()
	require.NoError(t, unitHolder.Pause(context.Background()))
	stage, result = unitHolder.Stage()
	require.Len(t, result.Errors, 0)
	require.Equal(t, metadata.StagePaused, stage)
	// pause again
	require.Error(t, unitHolder.Pause(context.Background()))
	stage, result = unitHolder.Stage()
	require.Len(t, result.Errors, 0)
	require.Equal(t, metadata.StagePaused, stage)

	// mock manually resume
	require.NoError(t, unitHolder.Resume(context.Background()))
	stage, result = unitHolder.Stage()
	require.Nil(t, result)
	require.Equal(t, metadata.StageRunning, stage)
	// resume again
	require.Error(t, unitHolder.Resume(context.Background()))

	// mock finished
	time.Sleep(time.Second)
	u.setResult(pb.ProcessResult{Errors: []*pb.ProcessError{{
		Message: "context canceled",
	}}})
	unitHolder.processWg.Wait()
	stage, result = unitHolder.Stage()
	require.Len(t, result.Errors, 0)
	require.Equal(t, metadata.StageFinished, stage)

	u.On("Status").Return(&pb.DumpStatus{})
	status := unitHolder.Status(context.Background())
	require.Equal(t, &pb.DumpStatus{}, status)

	// mock close
	require.NoError(t, unitHolder.Close(context.Background()))

	// mock pause after close
	require.EqualError(t, unitHolder.Pause(context.Background()), fmt.Sprintf("failed to pause unit with stage %d", metadata.StagePaused))
	// mock resume after close
	// depend on unit.Resume
	require.NoError(t, unitHolder.Resume(context.Background()))
}

func TestUnitHolderBinlog(t *testing.T) {
	unitHolder := &unitHolderImpl{}
	unitHolder.unit = &dumpling.Dumpling{}

	// wrong type
	msg, err := unitHolder.Binlog(context.Background(), &dmpkg.BinlogTaskRequest{})
	require.Error(t, err)
	require.Equal(t, "", msg)
	// no binlog error
	unitHolder.unit = syncer.NewSyncer(&config.SubTaskConfig{Flavor: mysql.MySQLFlavor}, nil, nil)
	unitHolder.runCtx = context.Background()
	msg, err = unitHolder.Binlog(context.Background(), &dmpkg.BinlogTaskRequest{})
	require.EqualError(t, err, "source '' has no error")
	require.Equal(t, "", msg)
	// binlog skip
	msg, err = unitHolder.Binlog(context.Background(), &dmpkg.BinlogTaskRequest{Op: pb.ErrorOp_Skip, BinlogPos: "mysql-bin.000001:2345"})
	require.Nil(t, err)
	require.Equal(t, "", msg)
}

func TestUnitHolderBinlogSchema(t *testing.T) {
	unitHolder := &unitHolderImpl{}
	unitHolder.unit = &dumpling.Dumpling{}

	// wrong type
	msg, err := unitHolder.BinlogSchema(context.Background(), &dmpkg.BinlogSchemaTaskRequest{})
	require.Error(t, err)
	require.Equal(t, "", msg)
	// wrong stage
	unitHolder.unit = syncer.NewSyncer(&config.SubTaskConfig{Flavor: mysql.MySQLFlavor}, nil, nil)
	unitHolder.runCtx = context.Background()
	msg, err = unitHolder.BinlogSchema(context.Background(), &dmpkg.BinlogSchemaTaskRequest{})
	require.EqualError(t, err, fmt.Sprintf("current stage is %d but not paused, invalid", metadata.StageRunning))
	require.Equal(t, "", msg)
	// binlog schema list
	unitHolder.result = &pb.ProcessResult{Errors: []*pb.ProcessError{{ErrCode: 1}}}
	msg, err = unitHolder.BinlogSchema(context.Background(), &dmpkg.BinlogSchemaTaskRequest{Op: pb.SchemaOp_RemoveSchema})
	require.Nil(t, err)
	require.Equal(t, "", msg)
}

type mockUnit struct {
	sync.Mutex
	mock.Mock
	resultCh chan pb.ProcessResult
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

func (u *mockUnit) Process(ctx context.Context, pr chan pb.ProcessResult) {
	u.Lock()
	defer u.Unlock()
	u.resultCh = pr
}

func (u *mockUnit) setResult(r pb.ProcessResult) {
	u.Lock()
	defer u.Unlock()
	u.resultCh <- r
}

func (u *mockUnit) Close() {}

func (u *mockUnit) Kill() {}

func (u *mockUnit) Pause() {}

func (u *mockUnit) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	u.Lock()
	defer u.Unlock()
	u.resultCh = pr
}

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

// Close implement Holder.Close
func (m *mockUnitHolder) Close(ctx context.Context) error {
	return nil
}

// Pause implement Holder.Pause
func (m *mockUnitHolder) Pause(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()
	return m.Called().Error(0)
}

// Resume implement Holder.Resume
func (m *mockUnitHolder) Resume(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()
	return m.Called().Error(0)
}

// Stage implement Holder.Stage
func (m *mockUnitHolder) Stage() (metadata.TaskStage, *pb.ProcessResult) {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	if result := args.Get(1); result != nil {
		return args.Get(0).(metadata.TaskStage), result.(*pb.ProcessResult)
	}
	return args.Get(0).(metadata.TaskStage), nil
}

// Status implement Holder.Status
func (m *mockUnitHolder) Status(ctx context.Context) interface{} {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0)
}

// Binlog implement Holder.Binlog
func (m *mockUnitHolder) Binlog(ctx context.Context, req *dmpkg.BinlogTaskRequest) (string, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0).(string), args.Error(1)
}

// BinlogSchema implement Holder.BinlogSchema
func (m *mockUnitHolder) BinlogSchema(ctx context.Context, req *dmpkg.BinlogSchemaTaskRequest) (string, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0).(string), args.Error(1)
}
