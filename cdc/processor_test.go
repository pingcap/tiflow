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
	"github.com/pingcap/tidb-cdc/cdc/model"
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/etcd"
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

var _ = check.Suite(&processorSuite{})

type processorTestCase struct {
	rawTxnTs         [][]uint64
	globalResolvedTs []uint64
	expect           [][]uint64
}

func (p *processorSuite) TestProcessor(c *check.C) {
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
	origfNewTsRw := fNewTsRWriter
	fNewTsRWriter = func(cli *clientv3.Client, changefeedID, captureID string) ProcessorTsRWriter {
		return &mockTsRWriter{}
	}
	defer func() {
		fCreateSchema = origFSchema
		fNewPDCli = origFNewPD
		fNewTsRWriter = origfNewTsRw
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

	var (
		l                sync.Mutex
		outputDML        []uint64
		outputResolvedTs []uint64
	)
	go func() {
		for {
			e, ok := <-p.ResolvedChan()
			if !ok {
				return
			}
			log.Info("output", zap.Reflect("ProcessorEntry", e))
			l.Lock()
			switch e.Typ {
			case ProcessorEntryDMLS:
				c.Assert(len(outputResolvedTs), check.Equals, 0)
				outputDML = append(outputDML, e.Ts)
			case ProcessorEntryResolved:
				outputResolvedTs = append(outputResolvedTs, e.Ts)
			}
			l.Unlock()
			p.ExecutedChan() <- e
		}
	}()
	for i, rawTxnTs := range cases.rawTxnTs {
		input := make(chan txn.RawTxn)
		p.SetInputChan(int64(i), input)
		go func(rawTxnTs []uint64) {
			for _, txnTs := range rawTxnTs {
				input <- txn.RawTxn{Ts: txnTs}
			}
		}(rawTxnTs)
	}
	for i, globalResolvedTs := range cases.globalResolvedTs {
		// hack to simulate owner to update global resolved ts
		p.(*processorImpl).getTsRwriter().(*mockTsRWriter).SetGlobalResolvedTs(globalResolvedTs)
		// waiting for processor push to resolvedTs
		for {
			l.Lock()
			needBreak := len(outputDML) == len(cases.expect[i]) && len(outputResolvedTs) > 0
			l.Unlock()
			if needBreak {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		l.Lock()
		sort.Slice(outputDML, func(i, j int) bool {
			return outputDML[i] < outputDML[j]
		})
		c.Assert(outputDML, check.DeepEquals, cases.expect[i])
		outputDML = []uint64{}
		c.Assert(outputResolvedTs, check.DeepEquals, []uint64{globalResolvedTs})
		outputResolvedTs = []uint64{}
		l.Unlock()
	}
	cancel()
	p.Close()
}
