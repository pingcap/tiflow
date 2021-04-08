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

package sorter

import (
	"context"
	"math"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	_ "net/http/pprof"
)

const (
	numProducers = 16
)

type sorterSuite struct{}

var _ = check.SerialSuites(&sorterSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func generateMockRawKV(ts uint64) *model.RawKVEntry {
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
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	conf := config.GetDefaultServerConfig()
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      60,
		MaxMemoryConsumption:   16 * 1024 * 1024 * 1024,
		NumWorkerPoolGoroutine: 4,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)
	sorter := NewUnifiedSorter("/tmp/sorter", "test-cf", "test", 0, "0.0.0.0:0")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	testSorter(ctx, c, sorter, 10000, true)
}

func (s *sorterSuite) TestSorterCancel(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	conf := config.GetDefaultServerConfig()
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      60,
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)
	sorter := NewUnifiedSorter("/tmp/sorter", "test-cf", "test", 0, "0.0.0.0:0")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	finishedCh := make(chan struct{})
	go func() {
		testSorter(ctx, c, sorter, 10000000, true)
		close(finishedCh)
	}()

	after := time.After(30 * time.Second)
	select {
	case <-after:
		c.Fatal("TestSorterCancel timed out")
	case <-finishedCh:
	}

	log.Info("Sorter successfully cancelled")
}

func testSorter(ctx context.Context, c *check.C, sorter puller.EventSorter, count int, needWorkerPool bool) {
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	if err != nil {
		log.Panic("Could not enable failpoint", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(ctx)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return sorter.Run(ctx)
	})

	if needWorkerPool {
		errg.Go(func() error {
			return RunWorkerPool(ctx)
		})
	}

	producerProgress := make([]uint64, numProducers)

	// launch the producers
	for i := 0; i < numProducers; i++ {
		finalI := i
		errg.Go(func() error {
			for j := 1; j <= count; j++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				sorter.AddEntry(ctx, model.NewPolymorphicEvent(generateMockRawKV(uint64(j)<<5)))
				if j%10000 == 0 {
					atomic.StoreUint64(&producerProgress[finalI], uint64(j)<<5)
				}
			}
			sorter.AddEntry(ctx, model.NewPolymorphicEvent(generateMockRawKV(uint64(count+1)<<5)))
			atomic.StoreUint64(&producerProgress[finalI], uint64(count+1)<<5)
			return nil
		})
	}

	// launch the resolver
	errg.Go(func() error {
		ticker := time.NewTicker(1 * time.Second)
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
				if resolvedTs == uint64(count)<<5 {
					return nil
				}
			}
		}
	})

	// launch the consumer
	errg.Go(func() error {
		counter := 0
		lastTs := uint64(0)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-sorter.Output():
				if event.RawKV.OpType != model.OpTypeResolved {
					if event.CRTs < lastTs {
						panic("regressed")
					}
					lastTs = event.CRTs
					counter += 1
					if counter%10000 == 0 {
						log.Debug("Messages received", zap.Int("counter", counter))
					}
					if counter >= numProducers*count {
						log.Debug("Unified Sorter test successful")
						cancel()
					}
				}
			case <-ticker.C:
				log.Debug("Consumer is alive")
			}
		}
	})

	err = errg.Wait()
	if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
		return
	}
	c.Assert(err, check.IsNil)
}

func (s *sorterSuite) TestSortDirConfigLocal(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	poolMu.Lock()
	// Clean up the back-end pool if one has been created
	pool = nil
	poolMu.Unlock()

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)
	// We expect the local setting to override the changefeed setting
	config.GetGlobalServerConfig().Sorter.SortDir = "/tmp/sorter_local"

	_ = NewUnifiedSorter("/tmp/sorter", /* the changefeed setting */
		"test-cf",
		"test",
		0,
		"0.0.0.0:0")

	poolMu.Lock()
	defer poolMu.Unlock()

	c.Assert(pool, check.NotNil)
	c.Assert(pool.dir, check.Equals, "/tmp/sorter_local")
}

func (s *sorterSuite) TestSortDirConfigChangeFeed(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	poolMu.Lock()
	// Clean up the back-end pool if one has been created
	pool = nil
	poolMu.Unlock()

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)
	// We expect the changefeed setting to take effect
	config.GetGlobalServerConfig().Sorter.SortDir = ""

	_ = NewUnifiedSorter("/tmp/sorter", /* the changefeed setting */
		"test-cf",
		"test",
		0,
		"0.0.0.0:0")

	poolMu.Lock()
	defer poolMu.Unlock()

	c.Assert(pool, check.NotNil)
	c.Assert(pool.dir, check.Equals, "/tmp/sorter")
}

// TestSorterCancelRestart tests the situation where the Unified Sorter is repeatedly canceled and
// restarted. There should not be any problem, especially file corruptions.
func (s *sorterSuite) TestSorterCancelRestart(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	conf := config.GetDefaultServerConfig()
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      0, // disable memory sort
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/asyncFlushStartDelay", "sleep(100)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/asyncFlushStartDelay")
	}()

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/asyncFlushInProcessDelay", "1%sleep(1)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/asyncFlushInProcessDelay")
	}()

	for i := 0; i < 5; i++ {
		sorter := NewUnifiedSorter("/tmp/sorter", "test-cf", "test", 0, "0.0.0.0:0")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		testSorter(ctx, c, sorter, 100000000, true)
		cancel()
	}
}
