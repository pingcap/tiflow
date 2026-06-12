// Copyright 2019 PingCAP, Inc.
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

package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// mocked loadMetaBinlog must be greater than relayHolderBinlog.
	loadMetaBinlog    = "(mysql-bin.00001,154)"
	relayHolderBinlog = "(mysql-bin.00001,150)"
)

func TestCreateUnits(t *testing.T) {
	cfg := &config.SubTaskConfig{
		Mode:   "xxx",
		Flavor: mysql.MySQLFlavor,
	}
	worker := "worker"
	require.Len(t, createUnits(cfg, nil, worker, nil), 0)

	cfg.Mode = config.ModeFull
	unitsFull := createUnits(cfg, nil, worker, nil)
	require.Len(t, unitsFull, 2)
	_, ok := unitsFull[0].(*dumpling.Dumpling)
	require.True(t, ok)
	_, ok = unitsFull[1].(*loader.LightningLoader)
	require.True(t, ok)

	cfg.Mode = config.ModeIncrement
	unitsIncr := createUnits(cfg, nil, worker, nil)
	require.Len(t, unitsIncr, 1)
	_, ok = unitsIncr[0].(*syncer.Syncer)
	require.True(t, ok)

	cfg.Mode = config.ModeAll
	unitsAll := createUnits(cfg, nil, worker, nil)
	require.Len(t, unitsAll, 3)
	_, ok = unitsAll[0].(*dumpling.Dumpling)
	require.True(t, ok)
	_, ok = unitsAll[1].(*loader.LightningLoader)
	require.True(t, ok)
	_, ok = unitsAll[2].(*syncer.Syncer)
	require.True(t, ok)
}

type MockUnit struct {
	processErrorCh chan error
	errInit        error
	errUpdate      error

	errFresh error

	typ     pb.UnitType
	isFresh bool
}

func NewMockUnit(typ pb.UnitType) *MockUnit {
	return &MockUnit{
		typ:            typ,
		processErrorCh: make(chan error),
		isFresh:        true,
	}
}

func (m *MockUnit) Init(_ context.Context) error {
	return m.errInit
}

func (m *MockUnit) Process(ctx context.Context, pr chan pb.ProcessResult) {
	select {
	case <-ctx.Done():
		pr <- pb.ProcessResult{
			IsCanceled: true,
			Errors:     nil,
		}
	case err := <-m.processErrorCh:
		if err == nil {
			pr <- pb.ProcessResult{}
		} else {
			pr <- pb.ProcessResult{
				Errors: []*pb.ProcessError{unit.NewProcessError(err)},
			}
		}
	}
}

func (m *MockUnit) Close() {}

func (m *MockUnit) Kill() {}

func (m MockUnit) Pause() {}

func (m *MockUnit) Resume(ctx context.Context, pr chan pb.ProcessResult) { m.Process(ctx, pr) }

func (m *MockUnit) Update(context.Context, *config.SubTaskConfig) error {
	return m.errUpdate
}

func (m *MockUnit) Status(_ *binlog.SourceStatus) interface{} {
	switch m.typ {
	case pb.UnitType_Check:
		return &pb.CheckStatus{}
	case pb.UnitType_Dump:
		return &pb.DumpStatus{}
	case pb.UnitType_Load:
		return &pb.LoadStatus{MetaBinlog: loadMetaBinlog}
	case pb.UnitType_Sync:
		return &pb.SyncStatus{}
	default:
		return struct{}{}
	}
}

func (m *MockUnit) Type() pb.UnitType { return m.typ }

func (m *MockUnit) IsFreshTask(_ context.Context) (bool, error) { return m.isFresh, m.errFresh }

func (m *MockUnit) InjectProcessError(ctx context.Context, err error) error {
	newCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	select {
	case <-newCtx.Done():
		return newCtx.Err()
	case m.processErrorCh <- err:
	}

	return nil
}

func (m *MockUnit) InjectInitError(err error) { m.errInit = err }

func (m *MockUnit) InjectUpdateError(err error) { m.errUpdate = err }

func (m *MockUnit) InjectFreshError(isFresh bool, err error) { m.isFresh, m.errFresh = isFresh, err }

type KillOrderUnit struct {
	typ pb.UnitType

	started chan struct{}
	ctx     context.Context

	canceledAtKill bool
}

func NewKillOrderUnit(typ pb.UnitType) *KillOrderUnit {
	return &KillOrderUnit{
		typ:     typ,
		started: make(chan struct{}),
	}
}

func (u *KillOrderUnit) Init(context.Context) error { return nil }

func (u *KillOrderUnit) Process(ctx context.Context, pr chan pb.ProcessResult) {
	u.ctx = ctx
	close(u.started)
	<-ctx.Done()
	pr <- pb.ProcessResult{IsCanceled: true}
}

func (u *KillOrderUnit) Close() {}

func (u *KillOrderUnit) Kill() {
	select {
	case <-u.ctx.Done():
		u.canceledAtKill = true
	default:
	}
}

func (u *KillOrderUnit) Pause() {}

func (u *KillOrderUnit) Resume(ctx context.Context, pr chan pb.ProcessResult) { u.Process(ctx, pr) }

func (u *KillOrderUnit) Update(context.Context, *config.SubTaskConfig) error { return nil }

func (u *KillOrderUnit) Status(*binlog.SourceStatus) interface{} { return &pb.SyncStatus{} }

func (u *KillOrderUnit) Type() pb.UnitType { return u.typ }

func (u *KillOrderUnit) IsFreshTask(context.Context) (bool, error) { return true, nil }

func TestSubTaskNormalUsage(t *testing.T) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTask(cfg, nil, "worker")
	require.Equal(t, pb.Stage_New, st.Stage())

	// test empty and fail
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return nil
	}
	st.Run(pb.Stage_Running, pb.Stage_Running, nil)
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.True(t, strings.Contains(st.Result().Errors[0].String(), "has no dm units for mode"))

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run(pb.Stage_Running, pb.Stage_Running, nil)
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.Nil(t, st.Result())

	// finish dump
	require.NoError(t, mockDumper.InjectProcessError(context.Background(), nil))
	for i := 0; i < 10; i++ {
		if st.CurrUnit().Type() == pb.UnitType_Load {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Nil(t, st.Result())
	require.Equal(t, pb.Stage_Running, st.Stage())

	// fail loader
	require.NoError(t, mockLoader.InjectProcessError(context.Background(), errors.New("loader process error")))
	for i := 0; i < 10; i++ {
		res := st.Result()
		if res != nil && st.Stage() == pb.Stage_Paused {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, mockLoader, st.CurrUnit())
	require.NotNil(t, st.Result())
	require.Len(t, st.Result().Errors, 1)
	require.True(t, strings.Contains(st.Result().Errors[0].Message, "loader process error"))
	require.Equal(t, pb.Stage_Paused, st.Stage())

	// restore from pausing
	require.NoError(t, st.Resume(nil))
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Nil(t, st.Result())
	require.Equal(t, pb.Stage_Running, st.Stage())

	// update in running
	require.NotNil(t, st.Update(context.Background(), nil))
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Nil(t, st.Result())
	require.Equal(t, pb.Stage_Running, st.Stage())

	// Pause
	require.NoError(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockLoader, st.CurrUnit())
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		t.Fatalf("result %+v is not right after closing", st.Result())
	}

	// update again
	require.NoError(t, st.Update(context.Background(), &config.SubTaskConfig{Name: "updateSubtask"}))
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockLoader, st.CurrUnit())
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		t.Fatalf("result %+v is not right after closing", st.Result())
	}

	// run again
	require.NoError(t, st.Resume(nil))
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Nil(t, st.Result())
	require.Equal(t, pb.Stage_Running, st.Stage())

	// pause again
	require.NoError(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockLoader, st.CurrUnit())
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		t.Fatalf("result %+v is not right after closing", st.Result())
	}

	// run again
	require.NoError(t, st.Resume(nil))
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Nil(t, st.Result())
	require.Equal(t, pb.Stage_Running, st.Stage())

	// finish loader
	require.NoError(t, mockLoader.InjectProcessError(context.Background(), nil))
	for i := 0; i < 1000; i++ {
		if st.Stage() == pb.Stage_Finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Equal(t, pb.Stage_Finished, st.Stage())
	require.Len(t, st.Result().Errors, 0)
}

func TestKillWithCauseDoesNotCancelBeforeKill(t *testing.T) {
	cfg := &config.SubTaskConfig{
		Name: "testKillOrder",
		Mode: config.ModeIncrement,
	}

	u := NewKillOrderUnit(pb.UnitType_Sync)

	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{u}
	}

	st := NewSubTask(cfg, nil, "worker")
	st.Run(pb.Stage_Running, pb.Stage_InvalidStage, nil)
	<-u.started

	st.killWithCause(errors.New("test"))
	require.False(t, u.canceledAtKill)
}

func TestPauseAndResumeSubtask(t *testing.T) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTask(cfg, nil, "worker")
	require.Equal(t, pb.Stage_New, st.Stage())

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run(pb.Stage_Running, pb.Stage_Running, nil)
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.Nil(t, st.Result())
	require.False(t, st.CheckUnit())

	// pause twice
	require.NoError(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		t.Fatalf("result %+v is not right after closing", st.Result())
	}

	require.NotNil(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		t.Fatalf("result %+v is not right after closing", st.Result())
	}

	// resume
	require.NoError(t, st.Resume(nil))
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.Nil(t, st.Result())

	require.NoError(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		t.Fatalf("result %+v is not right after closing", st.Result())
	}

	// resume
	require.NoError(t, st.Resume(nil))
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.Nil(t, st.Result())

	// fail dumper
	require.NoError(t, mockDumper.InjectProcessError(context.Background(), errors.New("dumper process error")))
	// InjectProcessError need 1 second, here we wait 1.5 second
	utils.WaitSomething(15, 100*time.Millisecond, func() bool {
		return st.Result() != nil
	})
	require.Equal(t, mockDumper, st.CurrUnit())
	require.NotNil(t, st.Result())
	require.Len(t, st.Result().Errors, 1)
	require.True(t, strings.Contains(st.Result().Errors[0].Message, "dumper process error"))
	require.Equal(t, pb.Stage_Paused, st.Stage())

	// pause
	require.NoError(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.NotNil(t, st.Result())
	require.Len(t, st.Result().Errors, 1)
	require.True(t, st.Result().IsCanceled)
	require.True(t, strings.Contains(st.Result().Errors[0].Message, "dumper process error"))

	// resume twice
	require.NoError(t, st.Resume(nil))
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.Nil(t, st.Result())

	require.NotNil(t, st.Resume(nil))
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.Nil(t, st.Result())
	// finish dump
	require.NoError(t, mockDumper.InjectProcessError(context.Background(), nil))
	utils.WaitSomething(20, 50*time.Millisecond, func() bool {
		return st.CurrUnit().Type() == pb.UnitType_Load
	})
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Nil(t, st.Result())
	require.Equal(t, pb.Stage_Running, st.Stage())

	// finish loader
	require.NoError(t, mockLoader.InjectProcessError(context.Background(), nil))
	utils.WaitSomething(20, 50*time.Millisecond, func() bool {
		return st.Stage() == pb.Stage_Finished
	})
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Equal(t, pb.Stage_Finished, st.Stage())
	require.Len(t, st.Result().Errors, 0)

	st.Run(pb.Stage_Finished, pb.Stage_Stopped, nil)
	require.Equal(t, mockLoader, st.CurrUnit())
	require.Equal(t, pb.Stage_Finished, st.Stage())
	require.Len(t, st.Result().Errors, 0)
}

func TestSubtaskWithStage(t *testing.T) {
	cfg := &config.SubTaskConfig{
		SourceID: "source",
		Name:     "testSubtaskScene",
		Mode:     config.ModeFull,
	}
	require.NoError(t, cfg.Adjust(false))

	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	require.Equal(t, pb.Stage_Paused, st.Stage())

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	// pause
	require.NotNil(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, nil, st.CurrUnit())
	require.Nil(t, st.Result())

	require.NoError(t, st.Resume(nil))
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	require.Nil(t, st.Result())

	// pause again
	require.NoError(t, st.Pause())
	require.Equal(t, pb.Stage_Paused, st.Stage())
	require.Equal(t, mockDumper, st.CurrUnit())
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		t.Fatalf("result %+v is not right after closing", st.Result())
	}

	st = NewSubTaskWithStage(cfg, pb.Stage_Finished, nil, "worker")
	require.Equal(t, pb.Stage_Finished, st.Stage())
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run(pb.Stage_Finished, pb.Stage_Stopped, nil)
	require.Equal(t, pb.Stage_Finished, st.Stage())
	require.Equal(t, nil, st.CurrUnit())
	require.Nil(t, st.Result())
}

func TestSubtaskFastQuit(t *testing.T) {
	// case: test subtask stuck into unitTransWaitCondition
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskFastQuit",
		Mode: config.ModeAll,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &SourceWorker{
		ctx: ctx,
		// loadStatus relay MetaBinlog must be greater
		relayHolder: NewDummyRelayHolderWithRelayBinlog(config.NewSourceConfig(), relayHolderBinlog),
	}
	InitConditionHub(w)

	mockLoader := NewMockUnit(pb.UnitType_Load)
	mockSyncer := NewMockUnit(pb.UnitType_Sync)

	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	st.prevUnit = mockLoader
	st.currUnit = mockSyncer

	finished := make(chan struct{})
	go func() {
		st.run()
		close(finished)
	}()

	// test Pause
	time.Sleep(time.Second) // wait for task to run for some time
	require.Equal(t, pb.Stage_Running, st.Stage())
	require.NoError(t, st.Pause())
	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("fail to pause subtask in 0.5s when stuck into unitTransWaitCondition")
	case <-finished:
	}
	require.Equal(t, pb.Stage_Paused, st.Stage())

	st = NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	st.units = []unit.Unit{mockLoader, mockSyncer}
	st.prevUnit = mockLoader
	st.currUnit = mockSyncer

	finished = make(chan struct{})
	go func() {
		st.run()
		close(finished)
	}()

	require.True(t, utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		return st.Stage() == pb.Stage_Running
	}))
	// test Close
	st.Close()
	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("fail to stop subtask in 0.5s when stuck into unitTransWaitCondition")
	case <-finished:
	}
	require.Equal(t, pb.Stage_Stopped, st.Stage())
}

func TestGetValidatorError(t *testing.T) {
	cfg := &config.SubTaskConfig{
		Name: "test-validate-error",
		ValidatorCfg: config.ValidatorConfig{
			Mode: config.ValidationFast,
		},
	}
	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	// validator == nil
	validatorErrs, err := st.GetValidatorError(pb.ValidateErrorState_InvalidErr)
	require.Nil(t, validatorErrs)
	require.True(t, terror.ErrValidatorNotFound.Equal(err))
	// validator != nil: will be tested in IT
}

func TestOperateValidatorError(t *testing.T) {
	cfg := &config.SubTaskConfig{
		Name: "test-validate-error",
		ValidatorCfg: config.ValidatorConfig{
			Mode: config.ValidationFast,
		},
	}
	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	// validator == nil
	require.True(t, terror.ErrValidatorNotFound.Equal(st.OperateValidatorError(pb.ValidationErrOp_ClearErrOp, 0, true)))
	// validator != nil: will be tested in IT
}

func TestValidatorStatus(t *testing.T) {
	cfg := &config.SubTaskConfig{
		Name: "test-validate-status",
		ValidatorCfg: config.ValidatorConfig{
			Mode: config.ValidationFast,
		},
	}
	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	// validator == nil
	stats, err := st.GetValidatorStatus()
	require.Nil(t, stats)
	require.True(t, terror.ErrValidatorNotFound.Equal(err))
	// validator != nil: will be tested in IT
}

func TestSubtaskRace(t *testing.T) {
	// to test data race of Marshal() and markResultCanceled()
	tempErrors := []*pb.ProcessError{}
	tempDetail := []byte{}
	tempProcessResult := pb.ProcessResult{
		IsCanceled: false,
		Errors:     tempErrors,
		Detail:     tempDetail,
	}
	cfg := &config.SubTaskConfig{
		Name: "test-subtask-race",
		ValidatorCfg: config.ValidatorConfig{
			Mode: config.ValidationFast,
		},
	}
	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	st.result = &tempProcessResult
	tempQueryStatusResponse := pb.QueryStatusResponse{}
	tempQueryStatusResponse.SubTaskStatus = make([]*pb.SubTaskStatus, 1)
	tempSubTaskStatus := pb.SubTaskStatus{}
	tempSubTaskStatus.Result = st.Result()
	tempQueryStatusResponse.SubTaskStatus[0] = &tempSubTaskStatus
	st.result.IsCanceled = false
	go func() {
		for i := 0; i < 10; i++ {
			_, _ = tempQueryStatusResponse.Marshal()
		}
	}()
	st.markResultCanceled()
	// this test is to test data race, so don't need assert here
}
