package main

import (
	"context"
	"flag"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
)


var sorterDir = flag.String("dir", "./sorter", "temporary directory used for sorting")
var numBatches = flag.Int("num-batches", 512, "number of batches of ordered events")
var msgsPerBatch = flag.Int("num-messages-per-batch", 102400, "number of events in a batch")
var bytesPerMsg = flag.Int("bytes-per-message", 1024, "number of bytes in an event")

func main() {
	flag.Parse()
	log.SetLevel(zap.DebugLevel)

	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	err := os.MkdirAll(*sorterDir, 0755)
	if err != nil {
		log.Error("sorter_stress_test:", zap.Error(err))
	}

 	sorter := puller.NewUnifiedSorter(*sorterDir)

 	eg, ctx := errgroup.WithContext(context.Background())

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
					if counter >= *numBatches * *msgsPerBatch {
						log.Debug("Unified Sorter test successful")
						os.Exit(0)
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
		log.Error("sorter_stress_test:", zap.Error(err))
	}
}

func generateGroup(ctx context.Context, sorter puller.EventSorter) {
	for i := 0; i < *msgsPerBatch; i++ {
		ts := (i << 5) + rand.Intn(256)
		event := model.NewPolymorphicEvent(newMockRawKV(uint64(ts)))
		sorter.AddEntry(ctx, event)
	}
}

var (
	key = []byte(randSeq(10))
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


