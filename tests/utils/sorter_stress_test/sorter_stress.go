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
	"math/rand"
	"net/http"
	_ "net/http/pprof" // #nosec G108
	"os"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/unified"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	sorterDir    = flag.String("dir", "./sorter", "temporary directory used for sorting")
	numBatches   = flag.Int("num-batches", 256, "number of batches of ordered events")
	msgsPerBatch = flag.Int("num-messages-per-batch", 1024, "number of events in a batch")
	bytesPerMsg  = flag.Int("bytes-per-message", 1024, "number of bytes in an event")
)

func main() {
	flag.Parse()
	log.SetLevel(zap.DebugLevel)
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	if err != nil {
		log.Fatal("Could not enable failpoint", zap.Error(err))
	}

	conf := config.GetDefaultServerConfig()
	conf.Sorter = &config.SorterConfig{
		NumConcurrentWorker:  8,
		ChunkSizeLimit:       1 * 1024 * 1024 * 1024,
		MaxMemoryPercentage:  60,
		MaxMemoryConsumption: 16 * 1024 * 1024 * 1024,
	}
	config.StoreGlobalServerConfig(conf)

	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	err = os.MkdirAll(*sorterDir, 0o700)
	if err != nil {
		log.Error("sorter_stress_test:", zap.Error(err))
	}

	sorter, err := unified.NewUnifiedSorter(*sorterDir, model.DefaultChangeFeedID("test-cf"), "test", 0)
	if err != nil {
		log.Panic("sorter_stress_test:", zap.Error(err))
	}

	ctx1, cancel := context.WithCancel(context.Background())

	eg, ctx := errgroup.WithContext(ctx1)

	eg.Go(func() error {
		return unified.RunWorkerPool(ctx)
	})

	eg.Go(func() error {
		return sorter.Run(ctx)
	})

	// launch the consumer
	eg.Go(func() error {
		counter := 0
		lastTs := uint64(0)
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
					if counter >= *numBatches**msgsPerBatch {
						log.Debug("Unified Sorter test successful")
						cancel()
						return nil
					}
				}
			}
		}
	})

	eg1 := errgroup.Group{}
	for i := 0; i < *numBatches; i++ {
		eg1.Go(func() error {
			generateGroup(ctx, sorter)
			return nil
		})
	}

	err = eg1.Wait()
	if err != nil {
		log.Error("sorter_stress_test:", zap.Error(err))
	}

	sorter.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, uint64((*msgsPerBatch<<5)+256)))

	err = eg.Wait()
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") {
			return
		}
		log.Error("sorter_stress_test:", zap.Error(err))
	}
}

func generateGroup(ctx context.Context, sorter sorter.EventSorter) {
	for i := 0; i < *msgsPerBatch; i++ {
		ts := (i << 5) + rand.Intn(256)
		event := model.NewPolymorphicEvent(newMockRawKV(uint64(ts)))
		sorter.AddEntry(ctx, event)
	}
}

var (
	key   = []byte(randSeq(10))
	value = []byte(randSeq(*bytesPerMsg))
)

func newMockRawKV(ts uint64) *model.RawKVEntry {
	return &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      key,
		Value:    value,
		OldValue: nil,
		StartTs:  ts - 5,
		CRTs:     ts,
		RegionID: 0,
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
