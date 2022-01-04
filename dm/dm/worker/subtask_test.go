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
	"time"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

const (
	// mocked loadMetaBinlog must be greater than relayHolderBinlog.
	loadMetaBinlog    = "(mysql-bin.00001,154)"
	relayHolderBinlog = "(mysql-bin.00001,150)"
)

type testSubTask struct{}

var _ = Suite(&testSubTask{})

func (t *testSubTask) TestCreateUnits(c *C) {
	cfg := &config.SubTaskConfig{
		Mode: "xxx",
	}
	worker := "worker"
	c.Assert(createUnits(cfg, nil, worker, nil), HasLen, 0)

	cfg.Mode = config.ModeFull
	unitsFull := createUnits(cfg, nil, worker, nil)
	c.Assert(unitsFull, HasLen, 2)
	_, ok := unitsFull[0].(*dumpling.Dumpling)
	c.Assert(ok, IsTrue)
	_, ok = unitsFull[1].(*loader.LightningLoader)
	c.Assert(ok, IsTrue)

	cfg.Mode = config.ModeIncrement
	unitsIncr := createUnits(cfg, nil, worker, nil)
	c.Assert(unitsIncr, HasLen, 1)
	_, ok = unitsIncr[0].(*syncer.Syncer)
	c.Assert(ok, IsTrue)

	cfg.Mode = config.ModeAll
	unitsAll := createUnits(cfg, nil, worker, nil)
	c.Assert(unitsAll, HasLen, 3)
	_, ok = unitsAll[0].(*dumpling.Dumpling)
	c.Assert(ok, IsTrue)
	_, ok = unitsAll[1].(*loader.LightningLoader)
	c.Assert(ok, IsTrue)
	_, ok = unitsAll[2].(*syncer.Syncer)
	c.Assert(ok, IsTrue)
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

func (t *testSubTask) TestSubTaskNormalUsage(c *C) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTask(cfg, nil, "worker")
	c.Assert(st.Stage(), DeepEquals, pb.Stage_New)

	// test empty and fail
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return nil
	}
	st.Run(pb.Stage_Running, nil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(strings.Contains(st.Result().Errors[0].String(), "has no dm units for mode"), IsTrue)

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run(pb.Stage_Running, nil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), IsNil)

	// finish dump
	c.Assert(mockDumper.InjectProcessError(context.Background(), nil), IsNil)
	for i := 0; i < 10; i++ {
		if st.CurrUnit().Type() == pb.UnitType_Load {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// fail loader
	c.Assert(mockLoader.InjectProcessError(context.Background(), errors.New("loader process error")), IsNil)
	for i := 0; i < 10; i++ {
		res := st.Result()
		if res != nil && st.Stage() == pb.Stage_Paused {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), NotNil)
	c.Assert(st.Result().Errors, HasLen, 1)
	c.Assert(strings.Contains(st.Result().Errors[0].Message, "loader process error"), IsTrue)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)

	// restore from pausing
	c.Assert(st.Resume(nil), IsNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// update in running
	c.Assert(st.Update(context.Background(), nil), NotNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// Pause
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// update again
	c.Assert(st.Update(context.Background(), &config.SubTaskConfig{Name: "updateSubtask"}), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// run again
	c.Assert(st.Resume(nil), IsNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// pause again
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// run again
	c.Assert(st.Resume(nil), IsNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// finish loader
	c.Assert(mockLoader.InjectProcessError(context.Background(), nil), IsNil)
	for i := 0; i < 1000; i++ {
		if st.Stage() == pb.Stage_Finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.Result().Errors, HasLen, 0)
}

func (t *testSubTask) TestPauseAndResumeSubtask(c *C) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTask(cfg, nil, "worker")
	c.Assert(st.Stage(), DeepEquals, pb.Stage_New)

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run(pb.Stage_Running, nil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.CheckUnit(), IsFalse)

	cfg1 := &config.SubTaskConfig{
		Name: "xxx",
		Mode: config.ModeFull,
		From: config.DBConfig{
			Host: "127.0.0.1",
		},
	}
	c.Assert(st.UpdateFromConfig(cfg1), IsNil)
	c.Assert(st.cfg.From, DeepEquals, cfg1.From)
	c.Assert(st.cfg.Name, Equals, "testSubtaskScene")

	// pause twice
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	c.Assert(st.Pause(), NotNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// resume
	c.Assert(st.Resume(nil), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), IsNil)

	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// resume
	c.Assert(st.Resume(nil), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), IsNil)

	// fail dumper
	c.Assert(mockDumper.InjectProcessError(context.Background(), errors.New("dumper process error")), IsNil)
	// InjectProcessError need 1 second, here we wait 1.5 second
	utils.WaitSomething(15, 100*time.Millisecond, func() bool {
		return st.Result() != nil
	})
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), NotNil)
	c.Assert(st.Result().Errors, HasLen, 1)
	c.Assert(strings.Contains(st.Result().Errors[0].Message, "dumper process error"), IsTrue)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)

	// pause
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), NotNil)
	c.Assert(st.Result().Errors, HasLen, 1)
	c.Assert(st.Result().IsCanceled, IsTrue)
	c.Assert(strings.Contains(st.Result().Errors[0].Message, "dumper process error"), IsTrue)

	// resume twice
	c.Assert(st.Resume(nil), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), IsNil)

	c.Assert(st.Resume(nil), NotNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), IsNil)
	// finish dump
	c.Assert(mockDumper.InjectProcessError(context.Background(), nil), IsNil)
	utils.WaitSomething(20, 50*time.Millisecond, func() bool {
		return st.CurrUnit().Type() == pb.UnitType_Load
	})
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// finish loader
	c.Assert(mockLoader.InjectProcessError(context.Background(), nil), IsNil)
	utils.WaitSomething(20, 50*time.Millisecond, func() bool {
		return st.Stage() == pb.Stage_Finished
	})
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.Result().Errors, HasLen, 0)

	st.Run(pb.Stage_Finished, nil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.Result().Errors, HasLen, 0)
}

func (t *testSubTask) TestSubtaskWithStage(c *C) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	c.Assert(st.Stage(), DeepEquals, pb.Stage_Paused)

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	// pause
	c.Assert(st.Pause(), NotNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, nil)
	c.Assert(st.Result(), IsNil)

	c.Assert(st.Resume(nil), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), IsNil)

	// pause again
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	st = NewSubTaskWithStage(cfg, pb.Stage_Finished, nil, "worker")
	c.Assert(st.Stage(), DeepEquals, pb.Stage_Finished)
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run(pb.Stage_Finished, nil)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.CurrUnit(), Equals, nil)
	c.Assert(st.Result(), IsNil)
}

func (t *testSubTask) TestSubtaskFastQuit(c *C) {
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
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.Pause(), IsNil)
	select {
	case <-time.After(500 * time.Millisecond):
		c.Fatal("fail to pause subtask in 0.5s when stuck into unitTransWaitCondition")
	case <-finished:
	}
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)

	st = NewSubTaskWithStage(cfg, pb.Stage_Paused, nil, "worker")
	st.units = []unit.Unit{mockLoader, mockSyncer}
	st.prevUnit = mockLoader
	st.currUnit = mockSyncer

	finished = make(chan struct{})
	go func() {
		st.run()
		close(finished)
	}()

	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		return st.Stage() == pb.Stage_Running
	}), IsTrue)
	// test Close
	st.Close()
	select {
	case <-time.After(500 * time.Millisecond):
		c.Fatal("fail to stop subtask in 0.5s when stuck into unitTransWaitCondition")
	case <-finished:
	}
	c.Assert(st.Stage(), Equals, pb.Stage_Stopped)
}
