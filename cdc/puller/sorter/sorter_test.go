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
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"

	_ "net/http/pprof"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	conf.DataDir = "/tmp/cdc_data"
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      60,
		MaxMemoryConsumption:   16 * 1024 * 1024 * 1024,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	c.Assert(err, check.IsNil)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir, "test-cf", "test", 0, "0.0.0.0:0")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	err = testSorter(ctx, c, sorter, 10000, true)
	c.Assert(err, check.ErrorMatches, ".*context cancel.*")
}

func (s *sorterSuite) TestSorterCancel(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	conf := config.GetDefaultServerConfig()
	conf.DataDir = "/tmp/cdc_data"
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      60,
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	c.Assert(err, check.IsNil)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir, "test-cf", "test", 0, "0.0.0.0:0")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	finishedCh := make(chan struct{})
	go func() {
		err := testSorter(ctx, c, sorter, 10000000, true)
		c.Assert(err, check.ErrorMatches, ".*context deadline exceeded.*")
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

func testSorter(ctx context.Context, c *check.C, sorter puller.EventSorter, count int, needWorkerPool bool) error {
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	if err != nil {
		log.Panic("Could not enable failpoint", zap.Error(err))
	}

	c.Assert(failpoint.Enable("github.com/pingcap/ticdc/pkg/util/InjectCheckDataDirSatisfied", ""), check.IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/ticdc/pkg/util/InjectCheckDataDirSatisfied"), check.IsNil)
	}()

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

	return errg.Wait()
}

func (s *sorterSuite) TestSortDirConfigLocal(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	poolMu.Lock()
	// Clean up the back-end pool if one has been created
	pool = nil
	poolMu.Unlock()

	err := os.MkdirAll("/tmp/sorter_local", 0o755)
	c.Assert(err, check.IsNil)
	// We expect the local setting to override the changefeed setting
	config.GetGlobalServerConfig().Sorter.SortDir = "/tmp/sorter_local"

	_, err = NewUnifiedSorter("/tmp/sorter", /* the changefeed setting */
		"test-cf",
		"test",
		0,
		"0.0.0.0:0")
	c.Assert(err, check.IsNil)

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

	_, err = NewUnifiedSorter("/tmp/sorter", /* the changefeed setting */
		"test-cf",
		"test",
		0,
		"0.0.0.0:0")
	c.Assert(err, check.IsNil)

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
	conf.DataDir = "/tmp/cdc_data"
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      0, // disable memory sort
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
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
		sorter, err := NewUnifiedSorter(conf.Sorter.SortDir, "test-cf", "test", 0, "0.0.0.0:0")
		c.Assert(err, check.IsNil)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = testSorter(ctx, c, sorter, 100000000, true)
		c.Assert(err, check.ErrorMatches, ".*context deadline exceeded.*")
		cancel()
	}
}

func (s *sorterSuite) TestSorterIOError(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	conf := config.GetDefaultServerConfig()
	conf.DataDir = "/tmp/cdc_data"
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      60,
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	c.Assert(err, check.IsNil)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir, "test-cf", "test", 0, "0.0.0.0:0")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// enable the failpoint to simulate backEnd allocation error (usually would happen when creating a file)
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectErrorBackEndAlloc", "return(true)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectErrorBackEndAlloc")
	}()

	finishedCh := make(chan struct{})
	go func() {
		err := testSorter(ctx, c, sorter, 10000, true)
		c.Assert(err, check.ErrorMatches, ".*injected alloc error.*")
		close(finishedCh)
	}()

	after := time.After(60 * time.Second)
	select {
	case <-after:
		c.Fatal("TestSorterIOError timed out")
	case <-finishedCh:
	}

	UnifiedSorterCleanUp()
	_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectErrorBackEndAlloc")
	// enable the failpoint to simulate backEnd write error (usually would happen when writing to a file)
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectErrorBackEndWrite", "return(true)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectErrorBackEndWrite")
	}()

	sorter, err = NewUnifiedSorter(conf.Sorter.SortDir, "test-cf", "test", 0, "0.0.0.0:0")
	c.Assert(err, check.IsNil)

	finishedCh = make(chan struct{})
	go func() {
		err := testSorter(ctx, c, sorter, 10000, true)
		c.Assert(err, check.ErrorMatches, ".*injected write error.*")
		close(finishedCh)
	}()

	after = time.After(60 * time.Second)
	select {
	case <-after:
		c.Fatal("TestSorterIOError timed out")
	case <-finishedCh:
	}
}

func (s *sorterSuite) TestSorterErrorReportCorrect(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	conf := config.GetDefaultServerConfig()
	conf.DataDir = "/tmp/cdc_data"
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      60,
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	c.Assert(err, check.IsNil)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir, "test-cf", "test", 0, "0.0.0.0:0")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// enable the failpoint to simulate backEnd allocation error (usually would happen when creating a file)
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectHeapSorterExitDelay", "sleep(2000)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectHeapSorterExitDelay")
	}()

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectErrorBackEndAlloc", "return(true)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/InjectErrorBackEndAlloc")
	}()

	finishedCh := make(chan struct{})
	go func() {
		err := testSorter(ctx, c, sorter, 10000, true)
		c.Assert(err, check.ErrorMatches, ".*injected alloc error.*")
		close(finishedCh)
	}()

	after := time.After(60 * time.Second)
	select {
	case <-after:
		c.Fatal("TestSorterIOError timed out")
	case <-finishedCh:
	}
}

func (s *sorterSuite) TestSortClosedAddEntry(c *check.C) {
	defer testleak.AfterTest(c)()
	defer UnifiedSorterCleanUp()

	sorter, err := NewUnifiedSorter("/tmp/sorter",
		"test-cf",
		"test",
		0,
		"0.0.0.0:0")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	err = sorter.Run(ctx)
	c.Assert(err, check.ErrorMatches, ".*deadline.*")

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel1()
	for i := 0; i < 10000; i++ {
		sorter.AddEntry(ctx1, model.NewPolymorphicEvent(generateMockRawKV(uint64(i))))
	}

	select {
	case <-ctx1.Done():
		c.Fatal("TestSortClosedAddEntry timed out")
	default:
	}
	cancel1()
}
