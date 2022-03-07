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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tiflow/cdc/model"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type mockPuller struct {
	t          *testing.T
	inCh       chan *model.RawKVEntry
	outCh      chan *model.RawKVEntry
	resolvedTs model.Ts
}

func newMockPuller(t *testing.T, startTs model.Ts) *mockPuller {
	return &mockPuller{
		t:          t,
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
	require.Nil(m.t, err)
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

func TestPuller(t *testing.T) {
	startTs := uint64(10)
	mockPuller := newMockPuller(t, startTs)
	ctx := cdcContext.NewBackendContext4Test(true)
	p, err := newDDLPuller(ctx, startTs)
	require.Nil(t, err)
	p.(*ddlPullerImpl).puller = mockPuller
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Run(ctx)
		if errors.Cause(err) == context.Canceled {
			err = nil
		}
		require.Nil(t, err)
	}()
	defer wg.Wait()
	defer p.Close()

	// test initialize state
	resolvedTs, ddl := p.FrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)

	// test send resolvedTs
	mockPuller.appendResolvedTs(15)
	waitResolvedTsGrowing(t, p, 15)

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
	require.Equal(t, resolvedTs, uint64(15))
	require.Nil(t, ddl)

	mockPuller.appendResolvedTs(20)
	waitResolvedTsGrowing(t, p, 16)
	resolvedTs, ddl = p.FrontDDL()
	require.Equal(t, resolvedTs, uint64(16))
	require.Equal(t, ddl.ID, int64(1))
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(16))
	require.Equal(t, ddl.ID, int64(1))

	// DDL could be processed with a delay, wait here for a pending DDL job is added
	waitResolvedTsGrowing(t, p, 18)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(18))
	require.Equal(t, ddl.ID, int64(2))

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
	waitResolvedTsGrowing(t, p, 25)

	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(25))
	require.Equal(t, ddl.ID, int64(3))
	_, ddl = p.PopFrontDDL()
	require.Nil(t, ddl)

	waitResolvedTsGrowing(t, p, 30)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(30))
	require.Nil(t, ddl)

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
	waitResolvedTsGrowing(t, p, 40)
	resolvedTs, ddl = p.PopFrontDDL()
	// no ddl should be received
	require.Equal(t, resolvedTs, uint64(40))
	require.Nil(t, ddl)
}

func TestResolvedTsStuck(t *testing.T) {
	// For observing the logs
	zapcore, logs := observer.New(zap.WarnLevel)
	conf := &log.Config{Level: "warn", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	logger := zap.New(zapcore)
	restoreFn := log.ReplaceGlobals(logger, r)
	defer restoreFn()

	startTs := uint64(10)
	mockPuller := newMockPuller(t, startTs)
	ctx := cdcContext.NewBackendContext4Test(true)
	p, err := newDDLPuller(ctx, startTs)
	require.Nil(t, err)

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
		require.Nil(t, err)
	}()
	defer wg.Wait()
	defer p.Close()

	// test initialize state
	resolvedTs, ddl := p.FrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)

	mockPuller.appendResolvedTs(30)
	waitResolvedTsGrowing(t, p, 30)
	require.Equal(t, logs.Len(), 0)

	mockClock.Add(2 * ownerDDLPullerStuckWarnTimeout)
	for i := 0; i < 20; i++ {
		mockClock.Add(time.Second)
		if logs.Len() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
		if i == 19 {
			t.Fatal("warning log not printed")
		}
	}

	mockPuller.appendResolvedTs(40)
	waitResolvedTsGrowing(t, p, 40)
}

// waitResolvedTsGrowing can wait the first DDL reaches targetTs or if no pending
// DDL, DDL resolved ts reaches targetTs.
func waitResolvedTsGrowing(t *testing.T, p DDLPuller, targetTs model.Ts) {
	err := retry.Do(context.Background(), func() error {
		resolvedTs, _ := p.FrontDDL()
		if resolvedTs < targetTs {
			return errors.New("resolvedTs < targetTs")
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(100))
	require.Nil(t, err)
}
