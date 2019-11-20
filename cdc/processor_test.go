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

package cdc

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/check"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.uber.org/zap"
)

type processorSuite struct{}

type mockTsRWriter struct {
	resolvedTs       uint64
	checkpointTs     uint64
	globalResolvedTs uint64
	l                sync.Mutex
}

func (s *mockTsRWriter) WriteResolvedTs(ctx context.Context, resolvedTs uint64) error {
	s.l.Lock()
	defer s.l.Unlock()
	log.Info("write", zap.Uint64("localResolvedTs", resolvedTs))
	s.resolvedTs = resolvedTs
	return nil
}

func (s *mockTsRWriter) WriteCheckpointTs(ctx context.Context, checkpointTs uint64) error {
	s.l.Lock()
	defer s.l.Unlock()
	log.Info("write", zap.Uint64("checkpointTs", checkpointTs))
	s.checkpointTs = checkpointTs
	return nil
}

func (s *mockTsRWriter) ReadGlobalResolvedTs(ctx context.Context) (uint64, error) {
	s.l.Lock()
	defer s.l.Unlock()
	return s.globalResolvedTs, nil
}
func (s *mockTsRWriter) SetGlobalResolvedTs(ts uint64) {
	s.l.Lock()
	defer s.l.Unlock()
	s.globalResolvedTs = ts
}

// mockMounter pretend to decode a RawTxn by returning a Txn of the same Ts
type mockMounter struct{}

func (m mockMounter) Mount(rawTxn model.RawTxn) (*model.Txn, error) {
	return &model.Txn{Ts: rawTxn.Ts}, nil
}

// mockSinker append all received Txns for validation
type mockSinker struct {
	sink.Sink
	synced []model.Txn
	mu     sync.Mutex
}

func (m *mockSinker) Emit(ctx context.Context, t model.Txn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.synced = append(m.synced, t)
	return nil
}

var _ = check.Suite(&processorSuite{})

type processorTestCase struct {
	rawTxnTs         [][]uint64
	globalResolvedTs []uint64
	expect           [][]uint64
}

func (p *processorSuite) TestProcessor(c *check.C) {
	c.Skip("can't create mock puller")
	cases := &processorTestCase{
		rawTxnTs: [][]uint64{
			{1, 4, 7, 9, 12, 14, 16, 20},
			{2, 4, 8, 13, 24},
		},
		globalResolvedTs: []uint64{14, 15, 19},
		expect: [][]uint64{
			{1, 2, 4, 4, 7, 8, 9, 12, 13, 14},
			{},
			{16},
		},
	}
	runCase(c, cases)
}

func runCase(c *check.C, cases *processorTestCase) {
	origFSchema := fCreateSchema
	fCreateSchema = func(pdEndpoints []string) (*schema.Storage, error) {
		return nil, nil
	}
	origFNewPD := fNewPDCli
	fNewPDCli = func(pdAddrs []string, security pd.SecurityOption) (pd.Client, error) {
		return nil, nil
	}
	origFNewTsRw := fNewTsRWriter
	fNewTsRWriter = func(cli *clientv3.Client, changefeedID, captureID string) ProcessorTsRWriter {
		return &mockTsRWriter{}
	}
	origFNewMounter := fNewMounter
	fNewMounter = func(schema *schema.Storage, loc *time.Location) mounter {
		return mockMounter{}
	}
	origFNewSink := fNewMySQLSink
	sinker := &mockSinker{}
	fNewMySQLSink = func(sinkURI string, infoGetter sink.TableInfoGetter, opts map[string]string) (sink.Sink, error) {
		return sinker, nil
	}
	defer func() {
		fCreateSchema = origFSchema
		fNewPDCli = origFNewPD
		fNewTsRWriter = origFNewTsRw
		fNewMounter = origFNewMounter
		fNewMySQLSink = origFNewSink
	}()
	dir := c.MkDir()
	etcdURL, etcd, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	defer etcd.Close()

	p, err := NewProcessor([]string{etcdURL.String()}, model.ChangeFeedDetail{}, "", "")
	c.Assert(err, check.IsNil)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	p.Run(ctx, errCh)

	for i, rawTxnTs := range cases.rawTxnTs {
		input := make(chan model.RawTxn)
		p.SetInputChan(int64(i), input)
		go func(rawTxnTs []uint64) {
			for _, txnTs := range rawTxnTs {
				input <- model.RawTxn{Ts: txnTs}
			}
		}(rawTxnTs)
	}
	for i, globalResolvedTs := range cases.globalResolvedTs {
		// hack to simulate owner to update global resolved ts
		p.(*processorImpl).getTsRwriter().(*mockTsRWriter).SetGlobalResolvedTs(globalResolvedTs)
		// waiting for processor push to resolvedTs
		for {
			sinker.mu.Lock()
			needBreak := len(sinker.synced) == len(cases.expect[i])
			sinker.mu.Unlock()
			if needBreak {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		sinker.mu.Lock()
		syncedTs := make([]uint64, 0, len(sinker.synced))
		for _, s := range sinker.synced {
			syncedTs = append(syncedTs, s.Ts)
		}
		sort.Slice(syncedTs, func(i, j int) bool {
			return syncedTs[i] < syncedTs[j]
		})
		c.Assert(syncedTs, check.DeepEquals, cases.expect[i])
		sinker.synced = sinker.synced[:0]
		sinker.mu.Unlock()
	}
	cancel()
}
