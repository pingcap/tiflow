// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tiflow/cdc/model"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var _ = check.Suite(&ddlPullerSuite{})

type ddlPullerSuite struct {
}

type mockPuller struct {
	c          *check.C
	inCh       chan *model.RawKVEntry
	outCh      chan *model.RawKVEntry
	resolvedTs model.Ts
}

func newMockPuller(c *check.C, startTs model.Ts) *mockPuller {
	return &mockPuller{
		c:          c,
		inCh:       make(chan *model.RawKVEntry),
		outCh:      make(chan *model.RawKVEntry),
		resolvedTs: startTs - 1,
	}
}

func (m *mockPuller) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-m.inCh:
			m.outCh <- e
			atomic.StoreUint64(&m.resolvedTs, e.CRTs)
		}
	}
}

func (m *mockPuller) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&m.resolvedTs)
}

func (m *mockPuller) Output() <-chan *model.RawKVEntry {
	return m.outCh
}

func (m *mockPuller) IsInitialized() bool {
	return true
}

func (m *mockPuller) append(e *model.RawKVEntry) {
	m.inCh <- e
}

func (m *mockPuller) appendDDL(job *timodel.Job) {
	b, err := json.Marshal(job)
	m.c.Assert(err, check.IsNil)
	ek := []byte("m")
	ek = codec.EncodeBytes(ek, []byte("DDLJobList"))
	ek = codec.EncodeUint(ek, uint64('l'))
	ek = codec.EncodeInt(ek, 1)
	m.append(&model.RawKVEntry{
		OpType:  model.OpTypePut,
		Key:     ek,
		Value:   b,
		StartTs: job.StartTS,
		CRTs:    job.BinlogInfo.FinishedTS,
	})
}

func (m *mockPuller) appendResolvedTs(ts model.Ts) {
	m.append(&model.RawKVEntry{
		OpType:  model.OpTypeResolved,
		CRTs:    ts,
		StartTs: ts,
	})
}

func (s *ddlPullerSuite) TestPuller(c *check.C) {
	defer testleak.AfterTest(c)()
	startTs := uint64(10)
	mockPuller := newMockPuller(c, startTs)
	ctx := cdcContext.NewBackendContext4Test(true)
	p, err := newDDLPuller(ctx, startTs)
	c.Assert(err, check.IsNil)
	p.(*ddlPullerImpl).puller = mockPuller
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Run(ctx)
		if errors.Cause(err) == context.Canceled {
			err = nil
		}
		c.Assert(err, check.IsNil)
	}()
	defer wg.Wait()
	defer p.Close()

	// test initialize state
	resolvedTs, ddl := p.FrontDDL()
	c.Assert(resolvedTs, check.Equals, startTs)
	c.Assert(ddl, check.IsNil)
	resolvedTs, ddl = p.PopFrontDDL()
	c.Assert(resolvedTs, check.Equals, startTs)
	c.Assert(ddl, check.IsNil)

	// test send resolvedTs
	mockPuller.appendResolvedTs(15)
	waitResolvedTsGrowing(c, p, 15)

	// test send ddl job out of order
	mockPuller.appendDDL(&timodel.Job{
		ID:         2,
		Type:       timodel.ActionCreateTable,
		StartTS:    5,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 18},
	})
	mockPuller.appendDDL(&timodel.Job{
		ID:         1,
		Type:       timodel.ActionCreateTable,
		StartTS:    5,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 16},
	})
	resolvedTs, ddl = p.FrontDDL()
	c.Assert(resolvedTs, check.Equals, uint64(15))
	c.Assert(ddl, check.IsNil)

	mockPuller.appendResolvedTs(20)
	waitResolvedTsGrowing(c, p, 16)
	resolvedTs, ddl = p.FrontDDL()
	c.Assert(resolvedTs, check.Equals, uint64(16))
	c.Assert(ddl.ID, check.Equals, int64(1))
	resolvedTs, ddl = p.PopFrontDDL()
	c.Assert(resolvedTs, check.Equals, uint64(16))
	c.Assert(ddl.ID, check.Equals, int64(1))

	// DDL could be processed with a delay, wait here for a pending DDL job is added
	waitResolvedTsGrowing(c, p, 18)
	resolvedTs, ddl = p.PopFrontDDL()
	c.Assert(resolvedTs, check.Equals, uint64(18))
	c.Assert(ddl.ID, check.Equals, int64(2))

	// test add ddl job repeated
	mockPuller.appendDDL(&timodel.Job{
		ID:         3,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 25},
	})
	mockPuller.appendDDL(&timodel.Job{
		ID:         3,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 25},
	})
	mockPuller.appendResolvedTs(30)
	waitResolvedTsGrowing(c, p, 25)

	resolvedTs, ddl = p.PopFrontDDL()
	c.Assert(resolvedTs, check.Equals, uint64(25))
	c.Assert(ddl.ID, check.Equals, int64(3))
	_, ddl = p.PopFrontDDL()
	c.Assert(ddl, check.IsNil)

	waitResolvedTsGrowing(c, p, 30)
	resolvedTs, ddl = p.PopFrontDDL()
	c.Assert(resolvedTs, check.Equals, uint64(30))
	c.Assert(ddl, check.IsNil)

	// test add invalid ddl job
	mockPuller.appendDDL(&timodel.Job{
		ID:         4,
		Type:       timodel.ActionLockTable,
		StartTS:    20,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 35},
	})
	mockPuller.appendDDL(&timodel.Job{
		ID:         5,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateCancelled,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 36},
	})
	mockPuller.appendResolvedTs(40)
	waitResolvedTsGrowing(c, p, 40)
	resolvedTs, ddl = p.PopFrontDDL()
	// no ddl should be received
	c.Assert(resolvedTs, check.Equals, uint64(40))
	c.Assert(ddl, check.IsNil)
}

func (*ddlPullerSuite) TestResolvedTsStuck(c *check.C) {
	defer testleak.AfterTest(c)()
	// For observing the logs
	zapcore, logs := observer.New(zap.WarnLevel)
	conf := &log.Config{Level: "warn", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	logger := zap.New(zapcore)
	log.ReplaceGlobals(logger, r)
	defer func() {
		logger, r, err := log.InitLogger(conf)
		c.Assert(err, check.IsNil)
		log.ReplaceGlobals(logger, r)
	}()

	startTs := uint64(10)
	mockPuller := newMockPuller(c, startTs)
	ctx := cdcContext.NewBackendContext4Test(true)
	p, err := newDDLPuller(ctx, startTs)
	c.Assert(err, check.IsNil)

	mockClock := clock.NewMock()
	p.(*ddlPullerImpl).clock = mockClock

	p.(*ddlPullerImpl).puller = mockPuller
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Run(ctx)
		if errors.Cause(err) == context.Canceled {
			err = nil
		}
		c.Assert(err, check.IsNil)
	}()
	defer wg.Wait()
	defer p.Close()

	// test initialize state
	resolvedTs, ddl := p.FrontDDL()
	c.Assert(resolvedTs, check.Equals, startTs)
	c.Assert(ddl, check.IsNil)
	resolvedTs, ddl = p.PopFrontDDL()
	c.Assert(resolvedTs, check.Equals, startTs)
	c.Assert(ddl, check.IsNil)

	mockPuller.appendResolvedTs(30)
	waitResolvedTsGrowing(c, p, 30)
	c.Assert(logs.Len(), check.Equals, 0)

	mockClock.Add(2 * ownerDDLPullerStuckWarnTimeout)
	for i := 0; i < 20; i++ {
		mockClock.Add(time.Second)
		if logs.Len() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
		if i == 19 {
			c.Fatal("warning log not printed")
		}
	}

	mockPuller.appendResolvedTs(40)
	waitResolvedTsGrowing(c, p, 40)
}

// waitResolvedTsGrowing can wait the first DDL reaches targetTs or if no pending
// DDL, DDL resolved ts reaches targetTs.
func waitResolvedTsGrowing(c *check.C, p DDLPuller, targetTs model.Ts) {
	err := retry.Do(context.Background(), func() error {
		resolvedTs, _ := p.FrontDDL()
		if resolvedTs < targetTs {
			return errors.New("resolvedTs < targetTs")
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(100))
	c.Assert(err, check.IsNil)
}
