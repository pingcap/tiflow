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

package unified

import (
	"context"
	"math"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

const (
	numProducers = 16
)

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

func TestSorterBasic(t *testing.T) {
	defer CleanUp()

	conf := config.GetDefaultServerConfig()
	conf.DataDir = t.TempDir()
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPercentage:    60,
		MaxMemoryConsumption:   16 * 1024 * 1024 * 1024,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	require.Nil(t, err)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir,
		model.DefaultChangeFeedID("test-cf"), "test", 0)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err = testSorter(ctx, t, sorter, 10000)
	require.Regexp(t, ".*context cancel.*", err)
}

func TestSorterCancel(t *testing.T) {
	defer CleanUp()

	conf := config.GetDefaultServerConfig()
	conf.DataDir = t.TempDir()
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPercentage:    60,
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	require.Nil(t, err)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir,
		model.DefaultChangeFeedID("cf-1"),
		"test", 0)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	finishedCh := make(chan struct{})
	go func() {
		err := testSorter(ctx, t, sorter, 10000000)
		require.Regexp(t, ".*context deadline exceeded.*", err)
		close(finishedCh)
	}()

	after := time.After(30 * time.Second)
	select {
	case <-after:
		t.Fatal("TestSorterCancel timed out")
	case <-finishedCh:
	}

	log.Info("Sorter successfully cancelled")
}

func testSorter(ctx context.Context, t *testing.T, sorter sorter.EventSorter, count int) error {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	if err != nil {
		log.Panic("Could not enable failpoint", zap.Error(err))
	}

	p := "github.com/pingcap/tiflow/pkg/util/InjectCheckDataDirSatisfied"
	require.Nil(t, failpoint.Enable(p, ""))
	defer func() {
		require.Nil(t, failpoint.Disable(p))
	}()

	ctx, cancel := context.WithCancel(ctx)
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return sorter.Run(ctx)
	})
	errg.Go(func() error {
		return RunWorkerPool(ctx)
	})

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

func TestSortDirConfigChangeFeed(t *testing.T) {
	defer CleanUp()

	poolMu.Lock()
	// Clean up the back-end pool if one has been created
	pool = nil
	poolMu.Unlock()

	dir := t.TempDir()
	// We expect the changefeed setting to take effect
	config.GetGlobalServerConfig().Sorter.SortDir = ""

	_, err := NewUnifiedSorter(dir, /* the changefeed setting */
		model.DefaultChangeFeedID("cf-1"), "test", 0)
	require.Nil(t, err)

	poolMu.Lock()
	defer poolMu.Unlock()

	require.NotNil(t, pool)
	require.Equal(t, dir, pool.dir)
}

// TestSorterCancelRestart tests the situation where the Unified Sorter is repeatedly canceled and
// restarted. There should not be any problem, especially file corruptions.
func TestSorterCancelRestart(t *testing.T) {
	defer CleanUp()

	conf := config.GetDefaultServerConfig()
	conf.DataDir = t.TempDir()
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPercentage:    0, // disable memory sort
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	require.Nil(t, err)

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/asyncFlushStartDelay", "sleep(100)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/asyncFlushStartDelay")
	}()

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/asyncFlushInProcessDelay", "1%sleep(1)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/asyncFlushInProcessDelay")
	}()

	for i := 0; i < 5; i++ {
		sorter, err := NewUnifiedSorter(conf.Sorter.SortDir,
			model.DefaultChangeFeedID("cf-1"),
			"test", 0)
		require.Nil(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = testSorter(ctx, t, sorter, 100000000)
		require.Regexp(t, ".*context deadline exceeded.*", err)
		cancel()
	}
}

func TestSorterIOError(t *testing.T) {
	defer CleanUp()

	conf := config.GetDefaultServerConfig()
	conf.DataDir = t.TempDir()
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPercentage:    60,
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	require.Nil(t, err)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir,
		model.DefaultChangeFeedID("cf-1"),
		"test", 0)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// enable the failpoint to simulate backEnd allocation error (usually would happen when creating a file)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectErrorBackEndAlloc", "return(true)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectErrorBackEndAlloc")
	}()

	finishedCh := make(chan struct{})
	go func() {
		err := testSorter(ctx, t, sorter, 10000)
		require.Regexp(t, ".*injected alloc error.*", err)
		close(finishedCh)
	}()

	after := time.After(60 * time.Second)
	select {
	case <-after:
		t.Fatal("TestSorterIOError timed out")
	case <-finishedCh:
	}

	CleanUp()
	_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectErrorBackEndAlloc")
	// enable the failpoint to simulate backEnd write error (usually would happen when writing to a file)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectErrorBackEndWrite", "return(true)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectErrorBackEndWrite")
	}()

	// recreate the sorter
	sorter, err = NewUnifiedSorter(conf.Sorter.SortDir,
		model.DefaultChangeFeedID("cf-1"), "test", 0)
	require.Nil(t, err)

	finishedCh = make(chan struct{})
	go func() {
		err := testSorter(ctx, t, sorter, 10000)
		require.Regexp(t, ".*injected write error.*", err)
		close(finishedCh)
	}()

	after = time.After(60 * time.Second)
	select {
	case <-after:
		t.Fatal("TestSorterIOError timed out")
	case <-finishedCh:
	}
}

func TestSorterErrorReportCorrect(t *testing.T) {
	defer CleanUp()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	conf := config.GetDefaultServerConfig()
	conf.DataDir = t.TempDir()
	sortDir := filepath.Join(conf.DataDir, config.DefaultSortDir)
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPercentage:    60,
		MaxMemoryConsumption:   0,
		NumWorkerPoolGoroutine: 4,
		SortDir:                sortDir,
	}
	config.StoreGlobalServerConfig(conf)

	err := os.MkdirAll(conf.Sorter.SortDir, 0o755)
	require.Nil(t, err)
	sorter, err := NewUnifiedSorter(conf.Sorter.SortDir,
		model.DefaultChangeFeedID("cf-1"),
		"test", 0)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// enable the failpoint to simulate backEnd allocation error (usually would happen when creating a file)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectHeapSorterExitDelay", "sleep(2000)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectHeapSorterExitDelay")
	}()

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectErrorBackEndAlloc", "return(true)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/InjectErrorBackEndAlloc")
	}()

	finishedCh := make(chan struct{})
	go func() {
		err := testSorter(ctx, t, sorter, 10000)
		require.Regexp(t, ".*injected alloc error.*", err)
		close(finishedCh)
	}()

	after := time.After(60 * time.Second)
	select {
	case <-after:
		t.Fatal("TestSorterIOError timed out")
	case <-finishedCh:
	}
}

func TestSortClosedAddEntry(t *testing.T) {
	defer CleanUp()

	sorter, err := NewUnifiedSorter(t.TempDir(),
		model.DefaultChangeFeedID("cf-1"), "test", 0)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	err = sorter.Run(ctx)
	require.Regexp(t, ".*deadline.*", err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel1()
	for i := 0; i < 10000; i++ {
		sorter.AddEntry(ctx1, model.NewPolymorphicEvent(generateMockRawKV(uint64(i))))
	}

	select {
	case <-ctx1.Done():
		t.Fatal("TestSortClosedAddEntry timed out")
	default:
	}
	cancel1()
}

func TestUnifiedSorterFileLockConflict(t *testing.T) {
	defer CleanUp()

	dir := t.TempDir()
	backEndPool, err := newBackEndPool(dir)
	defer backEndPool.terminate()
	require.Nil(t, err)

	// GlobalServerConfig overrides dir parameter in NewUnifiedSorter.
	config.GetGlobalServerConfig().Sorter.SortDir = dir
	_, err = NewUnifiedSorter(dir,
		model.DefaultChangeFeedID("cf-1"), "test", 0)
	require.Regexp(t, ".*file lock conflict.*", err)
}
