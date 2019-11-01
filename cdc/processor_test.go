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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/tidb-cdc/cdc/schema"

	"github.com/pingcap/tidb-cdc/cdc/model"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"go.uber.org/zap"
)

type processorSuite struct{}

type mockTSRWriter struct {
	resolvedTS       uint64
	checkpointTS     uint64
	globalResolvedTS uint64
	l                sync.Mutex
}

func (s *mockTSRWriter) WriteResolvedTS(resolvedTS uint64) error {
	s.l.Lock()
	defer s.l.Unlock()
	log.Info("write", zap.Uint64("localResolvedTS", resolvedTS))
	s.resolvedTS = resolvedTS
	return nil
}

func (s *mockTSRWriter) WriteCheckpointTS(checkpointTS uint64) error {
	s.l.Lock()
	defer s.l.Unlock()
	log.Info("write", zap.Uint64("checkpointTS", checkpointTS))
	s.checkpointTS = checkpointTS
	return nil
}

func (s *mockTSRWriter) ReadGlobalResolvedTS() (uint64, error) {
	s.l.Lock()
	defer s.l.Unlock()
	return s.globalResolvedTS, nil
}
func (s *mockTSRWriter) SetGlobalResolvedTS(ts uint64) {
	s.l.Lock()
	defer s.l.Unlock()
	s.globalResolvedTS = ts
}

var _ = check.Suite(&processorSuite{})

type processorTestCase struct {
	rawTxnTS         [][]uint64
	globalResolvedTS []uint64
	expect           [][]uint64
}

func (p *processorSuite) TestProcessor(c *check.C) {
	cases := &processorTestCase{
		rawTxnTS: [][]uint64{
			{1, 4, 7, 9, 12, 14, 16, 20},
			{2, 4, 8, 13, 24},
		},
		globalResolvedTS: []uint64{14, 15, 19},
		expect: [][]uint64{
			{1, 2, 4, 4, 7, 8, 9, 12, 13, 14},
			{},
			{16},
		},
	}
	runCase(c, cases)
}

func runCase(c *check.C, cases *processorTestCase) {
	tsRW := &mockTSRWriter{}
	origfSchema := fCreateSchema
	fCreateSchema = func(pdEndpoints []string) (*schema.Schema, error) {
		return nil, nil
	}
	defer func() {
		fCreateSchema = origfSchema
	}()
	p, err := NewProcessor(tsRW, []string{}, model.ChangeFeedDetail{}, "", "")
	c.Assert(err, check.IsNil)
	var l sync.Mutex
	var outputDML []uint64
	var outputResolvedTS []uint64
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
				c.Assert(len(outputResolvedTS), check.Equals, 0)
				outputDML = append(outputDML, e.TS)
			case ProcessorEntryResolved:
				outputResolvedTS = append(outputResolvedTS, e.TS)
			}
			l.Unlock()
			p.ExecutedChan() <- e
		}
	}()
	for i, rawTxnTS := range cases.rawTxnTS {
		input := make(chan txn.RawTxn)
		p.SetInputChan(uint64(i), input)
		go func(rawTxnTS []uint64) {
			for _, txnTS := range rawTxnTS {
				input <- txn.RawTxn{TS: txnTS}
			}
		}(rawTxnTS)
	}
	for i, globalResolvedTS := range cases.globalResolvedTS {
		tsRW.SetGlobalResolvedTS(globalResolvedTS)
		// waiting for processor push to resolvedTS
		for {
			l.Lock()
			needBreak := len(outputDML) == len(cases.expect[i]) && len(outputResolvedTS) > 0
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
		c.Assert(outputResolvedTS, check.DeepEquals, []uint64{globalResolvedTS})
		outputResolvedTS = []uint64{}
		l.Unlock()
	}
	p.Close()
}
