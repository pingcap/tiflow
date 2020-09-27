// Copyright 2020 PingCAP, Inc.
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

package puller

import (
	"context"
	"go.uber.org/zap/zapcore"
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	numProducers = 10
	eventCountPerProducer = 10000000
)

type sorterSuite struct{}

var _ = check.Suite(&sorterSuite{})

func generateMockRawKV(ts uint64) *model.RawKVEntry{
	return &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      []byte{},
		Value:    []byte{},
		OldValue: nil,
		StartTs:  ts - 5,
		CRTs:     ts,
		RegionID: 0,
	}
}

func (s *sorterSuite) TestSorterBasic(c *check.C) {
	log.SetLevel(zapcore.DebugLevel)
	sorter := NewUnifiedSorter("./sorter")
	testSorter(c, sorter)
}

func testSorter(c *check.C, sorter EventSorter) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10 * time.Minute)
	defer cancel()

	errg, ctx := errgroup.WithContext(timeoutCtx)
	errg.Go(func() error {
		return sorter.Run(ctx)
	})

	producerProgress := make([]uint64, numProducers)

	// launch the producers
	for i := 0; i < numProducers; i++ {
		finalI := i
		errg.Go(func() error {
			for j := 0; j < eventCountPerProducer; j++ {
				sorter.AddEntry(ctx, model.NewPolymorphicEvent(generateMockRawKV(uint64(j) << 5)))
				if j % 10000 == 0 {
					atomic.StoreUint64(&producerProgress[finalI], uint64(j))
				}
			}
			return nil
		})
	}

	// launch the resolver
	errg.Go(func () error {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				resolvedTs := uint64(math.MaxUint64)
				for i := range producerProgress {
					ts := atomic.LoadUint64(&producerProgress[i])
					if resolvedTs > ts {
						resolvedTs = ts
					}
				}
				sorter.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, resolvedTs))
			}
		}
	})

	// launch the consumer
	errg.Go(func() error {
		counter := 0
		lastTs := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-sorter.Output():
				c.Assert(event.CRTs, check.GreaterEqual, lastTs)
				lastTs = event.CRTs
				if event.RawKV.OpType != model.OpTypeResolved {
					counter += 1
					if counter % 10000 == 0 {
						log.Debug("Messages received", zap.Int("counter", counter))
					}
					if counter >= numProducers * eventCountPerProducer {
						log.Debug("Unified Sorter test successful")
						return nil
					}
				}
			}
		}
	})


	err := errg.Wait()
	c.Assert(err, check.IsNil)
}
