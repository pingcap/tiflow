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

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	pullerSorter "github.com/pingcap/tiflow/cdc/puller/sorter"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

var (
	sorterDir          = flag.String("dir", "./sorter", "temporary directory used for sorting")
	numSorters         = flag.Int("num-sorters", 256, "number of instances of sorters")
	numEvents          = flag.Int("num-events-per-sorter", 10000, "number of events sent to a sorter")
	percentageResolves = flag.Int("percentage-resolve-events", 70, "percentage of resolved events")
)

func main() {
	flag.Parse()
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/puller/sorter/sorterDebug", "return(true)")
	if err != nil {
		log.Fatal("Could not enable failpoint", zap.Error(err))
	}
	log.SetLevel(zapcore.DebugLevel)

	conf := config.GetDefaultServerConfig()
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:    8,
		ChunkSizeLimit:         1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:      60,
		MaxMemoryConsumption:   16 * 1024 * 1024 * 1024,
		NumWorkerPoolGoroutine: 16,
	}
	config.StoreGlobalServerConfig(conf)

	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	err = os.MkdirAll(*sorterDir, 0o755)
	if err != nil {
		log.Error("sorter_stress_test:", zap.Error(err))
	}

	sorters := make([]puller.EventSorter, *numSorters)
	ctx0, cancel := context.WithCancel(context.Background())
	errg, ctx := errgroup.WithContext(ctx0)

	errg.Go(func() error {
		return pullerSorter.RunWorkerPool(ctx)
	})

	var finishCount int32
	for i := 0; i < *numSorters; i++ {
		sorters[i], err = pullerSorter.NewUnifiedSorter(*sorterDir,
			"test-cf",
			fmt.Sprintf("test-%d", i),
			model.TableID(i),
			"0.0.0.0:0")
		if err != nil {
			log.Panic("many_sorters", zap.Error(err))
		}
		finalI := i

		// run sorter
		errg.Go(func() error {
			return printError(sorters[finalI].Run(ctx))
		})

		// run producer
		errg.Go(func() error {
			for j := 0; j < *numEvents; j++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				ev := generateEvent(uint64(finalI), uint64(j<<5))
				sorters[finalI].AddEntry(ctx, ev)
			}
			sorters[finalI].AddEntry(ctx, model.NewResolvedPolymorphicEvent(uint64(finalI), uint64(((*numEvents)<<5)+1)))
			return nil
		})

		// run consumer
		errg.Go(func() error {
			for {
				var ev *model.PolymorphicEvent
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ev = <-sorters[finalI].Output():
				}

				if ev.CRTs == uint64(((*numEvents)<<5)+1) {
					log.Info("Sorter finished", zap.Int("sorter-id", finalI))
					if atomic.AddInt32(&finishCount, 1) == int32(*numSorters) {
						log.Info("Many Sorters test finished, cancelling all goroutines")
						cancel()
					}
					return nil
				}
			}
		})
	}

	_ = printError(errg.Wait())
	if atomic.LoadInt32(&finishCount) == int32(*numSorters) {
		log.Info("Test was successful!")
	}
}

func generateEvent(region uint64, ts uint64) *model.PolymorphicEvent {
	r := rand.Int() % 100
	if r < *percentageResolves {
		return model.NewResolvedPolymorphicEvent(region, ts)
	}
	return model.NewPolymorphicEvent(&model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      []byte("keykeykey"),
		Value:    []byte("valuevaluevalue"),
		OldValue: nil,
		StartTs:  ts - 5,
		CRTs:     ts,
		RegionID: region,
	})
}

// printError is a helper for tracing errors on function returns
func printError(err error) error {
	if err != nil && errors.Cause(err) != context.Canceled &&
		errors.Cause(err) != context.DeadlineExceeded &&
		!strings.Contains(err.Error(), "context canceled") &&
		!strings.Contains(err.Error(), "context deadline exceeded") &&
		cerrors.ErrWorkerPoolHandleCancelled.NotEqual(errors.Cause(err)) {

		log.Warn("Unified Sorter: Error detected", zap.Error(err))
	}
	return err
}
